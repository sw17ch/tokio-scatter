use bytes::BytesMut;
use std::task::{Context, Poll};
use std::{collections::HashMap, collections::HashSet, future::Future, pin::Pin};
use tokio::io::AsyncWrite;

/// An internal type used to track state on each individual writer.
struct Writer<W>
where
    W: AsyncWrite,
{
    /// How much data we've written to the writer. This helps us track when
    /// there's more data to send to this writer.
    written: usize,

    /// An error on the writer does not constitute an error for the whole
    /// `Scatter`. Instead, we capture the error, and stop sending data to the
    /// writer.
    error: Option<std::io::Error>,

    /// The underlying writer into which we send data.
    w: W,
}

impl<W> Writer<W>
where
    W: AsyncWrite,
{
    /// Create a new `Writer` state from a type implementing `AsyncWrite`.
    fn new(w: W) -> Self {
        Self {
            written: 0,
            error: None,
            w,
        }
    }
}

/// A type used to indicate the completed state of a writer. Writers can
/// complete successfully, or with an error.
pub enum Finished<W> {
    Ok {
        writer: W,
        written: usize,
    },
    Err {
        writer: W,
        written: usize,
        error: std::io::Error,
    },
}

impl<W> Finished<W> {
    /// The writer completed successfully.
    pub fn is_ok(&self) -> bool {
        if let Finished::Ok { .. } = self {
            true
        } else {
            false
        }
    }

    /// The writer completed with an error.
    pub fn is_err(&self) -> bool {
        if let Finished::Err { .. } = self {
            true
        } else {
            false
        }
    }

    /// The number of bytes sent to the writer successfully.
    pub fn written(&self) -> usize {
        match self {
            Finished::Ok { written, .. } => *written,
            Finished::Err { written, .. } => *written,
        }
    }

    /// The error the writer encountered, if any.
    pub fn err(&self) -> Option<&std::io::Error> {
        match self {
            Finished::Ok { .. } => None,
            Finished::Err { error, .. } => Some(error),
        }
    }
}

/// A type used to scatter data to many `AsyncWrite` instances from a single task.
pub struct Scatter<W>
where
    W: AsyncWrite,
{
    /// Each writer added to the scatter is assigned a monotonically increasing
    /// unique ID. The next ID to be issued is tracked here.
    next_id: u64,

    /// We accumulate all the data written into the `Scatter` here. This allows
    /// us to catch up writers added part-way through.
    buf: BytesMut,

    /// All writers are associated with a unique ID.
    writers: HashMap<u64, Writer<W>>,

    /// We gather the IDs of writers that are behind in one place.
    behind: HashSet<u64>,

    /// We gather the IDs of writers that are up-to-date/current in one place.
    current: HashSet<u64>,

    /// We gather the IDs of writers that have aborted due to errors in one place.
    errors: HashSet<u64>,

    /// When iterating through writers, we need to track which writers were
    /// moved to different sets, and remove them from the `behind` set. We can't
    /// remove while iterating, so we store the IDs to be removed here. We store
    /// it here rather than in the loop itself so that we can avoid
    /// re-allocating this `Vec` for each `Work` future.
    moved: Vec<u64>,
}

impl<W> Scatter<W>
where
    W: AsyncWrite + Unpin,
{
    /// Create a new `Scatter` type. This does no allocation.
    pub fn new() -> Self {
        Self {
            next_id: 0,
            buf: Default::default(),

            writers: Default::default(),

            behind: Default::default(),
            current: Default::default(),
            errors: Default::default(),

            moved: Default::default(),
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Attach a writer to the `Scatter`. If the `Scatter` has already received
    /// data, this writer will be caught up.
    pub fn subscribe(&mut self, writer: W) {
        let id = self.next_id();
        let w = Writer::new(writer);

        let r = self.writers.insert(id, w);
        debug_assert!(r.is_none());

        let r = self.behind.insert(id);
        debug_assert!(r);
    }

    /// Return a future that resolves when all clients have been caught up. This
    /// should be called occasionally in order for progress to be made.
    pub fn flush(&mut self) -> Flush<'_, W> {
        Flush { scatter: self }
    }

    fn move_current_to_behind(&mut self) {
        for k in self.current.drain() {
            let r = self.behind.insert(k);
            debug_assert!(r);
        }
    }

    /// Add more data to be scattered to all writers. This does not perform any
    /// IO on its own. A call to `flush()` is required to scatter the data.
    pub fn write(&mut self, data: &[u8]) {
        if !data.is_empty() {
            self.move_current_to_behind();
            self.buf.extend_from_slice(data);
        }
    }

    /// Return an iterator that drains all writers that have encountered errors.
    /// The order is unspecified.
    pub fn errors(&mut self) -> FinishedIter<'_, W> {
        FinishedIter {
            only_errors: true,
            scatter: self,
        }
    }

    /// Return a new type that prevents more data from being added, and allows
    /// finalizing actions to be taken.
    pub fn finish(self) -> Finishing<W> {
        Finishing { scatter: self }
    }
}

/// A type that allows a scatter operation to be finalized. No more data can be
/// written. Clients that have yet to receive all the data can still receive the
/// data. New clients can be added.
pub struct Finishing<W>
where
    W: AsyncWrite,
{
    scatter: Scatter<W>,
}

impl<W> Finishing<W>
where
    W: AsyncWrite + Unpin,
{
    /// Add a new writer that should have the existing data sent to it.
    pub fn subscribe(&mut self, writer: W) {
        self.scatter.subscribe(writer)
    }

    /// Create a future that will have flushed all existing data to the writers
    /// capable of receiving it.
    pub fn flush(&mut self) -> Flush<'_, W> {
        self.scatter.flush()
    }

    /// Return an iterator over all clients that have completed. The order is
    /// unspecified.
    pub fn finished(&mut self) -> FinishedIter<'_, W> {
        FinishedIter {
            only_errors: false,
            scatter: &mut self.scatter,
        }
    }

    /// Take the accumulated data. Do not finish scattering to remaining
    /// writers. All remaining clients will be dropped.
    pub fn take_without_flush(self) -> BytesMut {
        self.scatter.buf
    }

    /// Wait for the data to be sent to all writers, and then return the
    /// accumulated data.
    pub async fn take(mut self) -> BytesMut {
        self.flush().await;
        self.take_without_flush()
    }
}

/// An iterator of writers on which no more progress can be made. They are
/// either fully written, or have encountered an error.
pub struct FinishedIter<'s, W>
where
    W: AsyncWrite,
{
    only_errors: bool,
    scatter: &'s mut Scatter<W>,
}

impl<'s, W> Iterator for FinishedIter<'s, W>
where
    W: AsyncWrite,
{
    type Item = Finished<W>;

    fn next(&mut self) -> Option<Self::Item> {
        // First, check if we have any writers that have encountered errors.
        if let Some(id) = self.scatter.errors.iter().nth(0).map(|id| *id) {
            let r = self.scatter.errors.remove(&id);
            debug_assert!(r);

            let Writer { w, written, error } = self
                .scatter
                .writers
                .remove(&id)
                .expect("error ID missing from writers");
            let error = error.expect("Writer in error set does not have an error");

            return Some(Finished::Err {
                writer: w,
                written,
                error,
            });
        }

        // If we're only extracting errors, stop here. Don't look at up-to-date
        // writers.
        if self.only_errors {
            return None;
        }

        // Next, check if any writers have completed.
        if let Some(id) = self.scatter.current.iter().nth(0).map(|id| *id) {
            let r = self.scatter.current.remove(&id);
            debug_assert!(r);

            let Writer { w, written, error } = self
                .scatter
                .writers
                .remove(&id)
                .expect("current ID missing from writers");
            debug_assert!(error.is_none());

            return Some(Finished::Ok { writer: w, written });
        }

        // If there are no currents or errors, we have no more finished writers.
        None
    }
}

/// A future that resolves when the available data has been fully sent to all
/// clients that have not encountered errors.
pub struct Flush<'s, W>
where
    W: AsyncWrite,
{
    scatter: &'s mut Scatter<W>,
}

impl<'s, W> Future for Flush<'s, W>
where
    W: AsyncWrite + Unpin,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Scatter {
            buf,
            writers,
            behind,
            current,
            errors,
            moved,
            ..
        } = &mut *self.scatter;

        for id in behind.iter() {
            let writer = writers.get_mut(id).expect("ID not found in writers");

            // The written amount should never exceed the buffer length so far.
            debug_assert!(writer.written <= buf.len());

            let r = Pin::new(&mut writer.w).poll_write(cx, &buf[writer.written..]);
            match r {
                Poll::Ready(Ok(len)) => {
                    // Some data was written successfully.
                    writer.written += len;
                }
                Poll::Ready(Err(e)) => {
                    // An error was encountered.
                    writer.error = Some(e);
                }
                std::task::Poll::Pending => {
                    // The writer is not yet complete with the write operation.
                }
            }

            if writer.error.is_some() {
                let r = errors.insert(*id);
                debug_assert!(r);
                moved.push(*id);
            } else if writer.written == buf.len() {
                let r = current.insert(*id);
                debug_assert!(r);
                moved.push(*id);
            }
        }

        // Remove each ID that was moved to a different set.
        for id in moved.drain(..) {
            let r = behind.remove(&id);
            assert!(r);
        }

        // If no writers are behind, we're ready.
        if behind.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;
    use tokio::net::UnixStream;
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn test_things() {
        async fn sink(mut u: UnixStream, b: Arc<Barrier>) -> BytesMut {
            let mut buf = BytesMut::new();

            // Start by reading into our buffer. The first read should be 5 bytes.
            let len = u.read_buf(&mut buf).await.unwrap();
            assert_eq!(5, len);

            // Synchronize on the barrier.
            b.wait().await;

            // Read again. The second read should also be 5 bttes.
            let len = u.read_buf(&mut buf).await.unwrap();
            assert_eq!(5, len);

            // Return the buffer resulting from the two reads.
            buf
        }

        let barrier_source = Arc::new(Barrier::new(3));

        let (sink1_in, sink1_out) = UnixStream::pair().unwrap();
        let (sink2_in, sink2_out) = UnixStream::pair().unwrap();
        let (sink3_in, sink3_out) = UnixStream::pair().unwrap();

        let sink1 = sink(sink1_out, barrier_source.clone());
        let sink2 = sink(sink2_out, barrier_source.clone());

        let mut s = Scatter::new();

        s.subscribe(sink1_in);
        s.subscribe(sink2_in);
        s.subscribe(sink3_in);

        // We immediately close sink3_out by dropping it. This will cause it to
        // appear in the error list.
        std::mem::drop(sink3_out);

        let scatter = async move {
            // Write some data.
            s.write(b"hello");

            // Wait for it to be written to all writers.
            s.flush().await;

            // Before writing more, make sure they've all read what we've written.
            barrier_source.wait().await;

            // Once all writers are caught up, write more.
            s.write(b"world");

            // Close the Scatter by calling finish. The new type prevents new
            // data from being written.
            let mut f = s.finish();

            // Finish writing out all remaining data to the writers.
            f.flush().await;

            // Extract each of the writers. We expect 2 to have succeeded, and 1
            // to have failed.
            let mut finished = f.finished();

            // Errors are emitted first.
            let n = finished.next().unwrap();
            assert!(n.is_err());
            assert_eq!(0, n.written());

            // Successful writers are emitted next.
            let n = finished.next().unwrap();
            assert!(n.is_ok());
            assert_eq!(10, n.written());
            let n = finished.next().unwrap();
            assert!(n.is_ok());
            assert_eq!(10, n.written());

            // When there are no more finished writers, we get None.
            let n = finished.next();
            assert!(n.is_none());

            // Finally, take out the buffer, and make sure it matches what we
            // expect.
            assert_eq!(b"helloworld", &f.take().await[..]);
        };

        let sink1_res = tokio::spawn(sink1);
        let sink2_res = tokio::spawn(sink2);
        let scatter_res = tokio::spawn(scatter);

        assert_eq!(b"helloworld", &sink1_res.await.unwrap()[..]);
        assert_eq!(b"helloworld", &sink2_res.await.unwrap()[..]);
        assert_eq!((), scatter_res.await.unwrap());
    }
}

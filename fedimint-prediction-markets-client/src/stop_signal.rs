
use tokio::spawn;
use tokio::sync::mpsc;

pub fn new() -> (Sender, Reciever) {
    let (tx, rx) = mpsc::channel::<CloseConfirmationWrapper>(1);

    (Sender(tx), Reciever(rx))
}

#[derive(Debug)]
pub struct Reciever(pub mpsc::Receiver<CloseConfirmationWrapper>);

#[derive(Debug)]
pub struct Sender(mpsc::Sender<CloseConfirmationWrapper>);
impl Sender {
    pub async fn wait_close(self) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.0.send(CloseConfirmationWrapper(tx)).await?;
        rx.recv().await.unwrap();

        Ok(())
    }
}

#[derive(Debug)]
pub struct CloseConfirmationWrapper(mpsc::Sender<()>);
impl Drop for CloseConfirmationWrapper {
    fn drop(&mut self) {
        let tx = self.0.clone();
        spawn(async move {
            _ = tx.send(()).await;
        });
    }
}

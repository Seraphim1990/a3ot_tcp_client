use std::cell::RefCell;
use tokio::net::TcpStream;
use tokio::io::{Interest, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use std::net::{SocketAddr, Ipv4Addr};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::io::AsyncReadExt;
pub use a3ot_modbus_protocol::{ModbusTCPUnit, ModbusTransportError, RegisterType, ModbusUnitError};
// ============ ПОМИЛКИ ============


#[derive(thiserror::Error, Debug)]
pub enum ModbusTCPError {
    #[error("Invalid Ip address")]
    InvalidIpAddr,

    #[error("Invalid Port")]
    InvalidPort,

    #[error("Already subscribed")]
    AlreadySubscribed,

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Not connected")]
    NotConnected,

    #[error("{0}")]
    ModbusUnitError(ModbusTransportError),

    #[error("Transport error: {0}")]
    ModbusClientError(String),
}

// ============ СТАН СОКЕТУ ============

#[derive(Debug, Clone, Copy)]
pub enum SocketStateChange {
    Disconnected,
    Connecting,
    Connected,
}
struct ModbusCommand {
    packet: Vec<u8>,  // Байти для відправки (з ModbusTCPUnit::build())
    reply: oneshot::Sender<Result<Vec<u8>, ModbusTCPError>>,  // Для відповіді
}
// ============ КЛІЄНТ ============

pub struct ModbusTCPClient {
    socket_addr: SocketAddr,
    reconnect_timeout: Option<Duration>,

    // Для підписки на стан
    state_rx: RefCell<Option<mpsc::Receiver<SocketStateChange>>>,

    cmd_tx: RefCell<Option<mpsc::Sender<ModbusCommand>>>,

    // Для управління worker
    worker_handle: RefCell<Option<JoinHandle<()>>>,
    shutdown_tx: RefCell<Option<mpsc::Sender<()>>>,

    retry_count: usize,
    retry_interval: Duration,
}

impl ModbusTCPClient {
    pub fn builder() -> ModbusTCPClientBuilder {
        ModbusTCPClientBuilder::new()
    }

    // ========== ПІДПИСКА НА ПОДІЇ СТАНУ ==========
    pub fn subscribe(&self) -> Result<mpsc::Receiver<SocketStateChange>, ModbusTCPError> {
        self.state_rx.borrow_mut()
            .take()
            .ok_or(ModbusTCPError::AlreadySubscribed)
    }

    // ========== ПІДКЛЮЧЕННЯ ==========
    pub async fn connect(&self) -> Result<(), ModbusTCPError> {
        // Канали
        let (state_tx, state_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let (cmd_tx, cmd_rx) = mpsc::channel(1);

        *self.cmd_tx.borrow_mut() = Some(cmd_tx);

        // Зберігаємо state_rx для subscribe()
        *self.state_rx.borrow_mut() = Some(state_rx);
        *self.shutdown_tx.borrow_mut() = Some(shutdown_tx);

        // Запускаємо worker
        let addr = self.socket_addr;
        let reconnect_timeout = self.reconnect_timeout;

        let retry_count = self.retry_count;
        let retry_interval = self.retry_interval;

        let handle = tokio::spawn(async move {
            Self::worker_task(addr, reconnect_timeout, state_tx, shutdown_rx, cmd_rx, retry_count, retry_interval).await;
        });

        *self.worker_handle.borrow_mut() = Some(handle);

        Ok(())
    }

    // ========== ВІДКЛЮЧЕННЯ ==========
    pub async fn disconnect(&self) -> Result<(), ModbusTCPError> {
        // Відправляємо shutdown
        if let Some(tx) = self.shutdown_tx.borrow_mut().take() {
            let _ = tx.send(()).await;
        }

        // Чекаємо завершення worker
        if let Some(handle) = self.worker_handle.borrow_mut().take() {
            let _ = handle.await;
        }

        Ok(())
    }

    // ========== WORKER TASK ==========
    async fn worker_task(
        addr: SocketAddr,
        reconnect_timeout: Option<Duration>,
        state_tx: mpsc::Sender<SocketStateChange>,
        mut shutdown_rx: mpsc::Receiver<()>,
        mut cmd_rx: mpsc::Receiver<ModbusCommand>,
        retry_count: usize,
        retry_interval: Duration,
    ) {
        let mut stream: Option<TcpStream> = None;
        let mut check_interval = tokio::time::interval(Duration::from_secs(5));
        let mut should_reconnect = false;

        // Підключаємось спочатку
        stream = Self::initial_connect(addr, reconnect_timeout, &state_tx, &mut shutdown_rx).await;

        // Якщо отримали None - shutdown під час initial connect
        if stream.is_none() {
            return;
        }

        'main: loop {
            // ⭐ Reconnect ДО select! (не блокує інші гілки)
            if should_reconnect && reconnect_timeout.is_some() {
                stream = Self::reconnect_loop(
                    addr,
                    reconnect_timeout.unwrap(),
                    &state_tx,
                    &mut shutdown_rx
                ).await;

                // Якщо None - shutdown під час reconnect
                if stream.is_none() {
                    break 'main;
                }
                should_reconnect = false;
            }

            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                if let Some(ref mut s) = stream {
                    // Виконуємо команду з retry
                    let result = Self::execute_command(
                        s,
                        cmd.packet,
                        retry_count,
                        retry_interval,
                    ).await;

                    // Відправляємо результат
                    let _ = cmd.reply.send(result);
                } else {
                    // Немає з'єднання
                    let _ = cmd.reply.send(Err(ModbusTCPError::NotConnected));
                }
            }


                // Перевірка сокету
                _ = check_interval.tick() => {
                    if let Some(ref s) = stream {
                        if !Self::check_socket_alive(s).await {
                            println!("⚠️ Socket died!");
                            let _ = state_tx.send(SocketStateChange::Disconnected).await;
                            stream = None;

                            if reconnect_timeout.is_some() {
                                should_reconnect = true; // ⭐ Встановлюємо флаг
                            } else {
                                break 'main; // Немає auto-reconnect - виходимо
                            }
                        }
                    }
                }

                // Shutdown
                _ = shutdown_rx.recv() => {
                    drop(stream);
                    break 'main; // ⭐ Виходимо з головного loop
                }
            }
        }

    }

    async fn execute_command(
        stream: &mut TcpStream,
        packet: Vec<u8>,
        retry_count: usize,
        retry_interval: Duration,
    ) -> Result<Vec<u8>, ModbusTCPError> {
        // for attempt in 0..=retry_count {
        //     // Відправляємо
        //     if let Err(e) = stream.write_all(&packet).await {
        //         if attempt == retry_count {
        //             return Err(ModbusTCPError::ConnectionFailed(e.to_string()));
        //         }
                    //         tokio::time::sleep(retry_interval).await;
        //         continue;
        //     }
                //
                //     // Читаємо відповідь з таймаутом
                //     let mut response = vec![0u8; 1024];
//
        //     match tokio::time::timeout(
        //         // Duration::from_secs(5), // Таймаут на читання
        //         retry_interval,
        //         stream.read(&mut response)
            //     ).await {
        //         Ok(Ok(n @ 1..)) => {
        //             response.truncate(n);
        //             return Ok(response);
        //         }
                    //         Ok(Ok(0)) => {
        //             // EOF - з'єднання закрилось
        //             return Err(ModbusTCPError::ConnectionFailed("Connection closed".to_string()));
        //         }
        //         Ok(Err(e)) => {
        //             if attempt == retry_count {
        //                 return Err(ModbusTCPError::ConnectionFailed(e.to_string()));
        //             }
                        //         }
                    //         Err(_) => {
        //             // Timeout
        //             if attempt == retry_count {
        //                 return Err(ModbusTCPError::ConnectionFailed("Timeout".to_string()));
        //             }
        //         }
        //     }
                //
                //     // Чекаємо перед наступною спробою
                //     if attempt < retry_count {
        //         tokio::time::sleep(retry_interval).await;
        //     }
                // }
        let mut response = Vec::with_capacity(1024);
        for attempt in 0..=retry_count {

            match tokio::time::timeout(Duration::from_secs(1), stream.write_all(&packet)).await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => {
                    if attempt == retry_count {
                        return Err(ModbusTCPError::ConnectionFailed(e.to_string()));
                    }
                    continue;
                },
                Err(_) => {
                    if attempt == retry_count {
                        return Err(ModbusTCPError::ConnectionFailed("Write timeout".to_string()));
                    }
                    continue;
                }
            }


            // 1. Відправка
            // if let Err(e) = stream.write_all(&packet).await {
            //     if attempt == retry_count {
            //         return Err(ModbusTCPError::ConnectionFailed(e.to_string()));
            //     }
            //     tokio::time::sleep(retry_interval).await;
            //     continue; // важливо!
            // }

            // 2. Чекати відповідь з таймаутом
            response.clear();
            match tokio::time::timeout(retry_interval, stream.read(&mut response)).await {
                Ok(Ok(n @ 1..))=> {
                    response.truncate(n);
                    return Ok(response);
                }
                Ok(Ok(0)) => {
                    return Err(ModbusTCPError::ConnectionFailed("Connection closed".to_string()));
                }
                Ok(Err(e)) => {
                    if attempt == retry_count {
                        return Err(ModbusTCPError::ConnectionFailed(e.to_string()));
                    }
                }
                Err(_) => {
                    // Timeout
                    if attempt == retry_count {
                        return Err(ModbusTCPError::ConnectionFailed("Timeout".to_string()));
                    }
                }
            }

        }

        Err(ModbusTCPError::ConnectionFailed("Max retries exceeded".to_string()))
    }

    async fn initial_connect(
        addr: SocketAddr,
        reconnect_timeout: Option<Duration>,
        state_tx: &mpsc::Sender<SocketStateChange>,
        shutdown_rx: &mut mpsc::Receiver<()>,
    ) -> Option<TcpStream> {
        if let Some(timeout) = reconnect_timeout {
            // З auto-reconnect - пробуємо до успіху або shutdown
            loop {
                tokio::select! {
                    result = Self::connect_with_retry(addr, state_tx) => {
                        if let Some(s) = result {
                            return Some(s);
                        }
                        // Не вдалось - чекаємо timeout
                    }

                    _ = shutdown_rx.recv() => {
                        // Shutdown під час initial connect
                        return None;
                    }
                }
                // Чекаємо перед наступною спробою
                tokio::select! {
                    _ = tokio::time::sleep(timeout) => {}
                    _ = shutdown_rx.recv() => {
                        return None;
                    }
                }
            }
        } else {
            // Без auto-reconnect - одна спроба
            tokio::select! {
                result = Self::connect_with_retry(addr, state_tx) => result,
                _ = shutdown_rx.recv() => None,
            }
        }
    }

    async fn reconnect_loop(
        addr: SocketAddr,
        timeout: Duration,
        state_tx: &mpsc::Sender<SocketStateChange>,
        shutdown_rx: &mut mpsc::Receiver<()>,
    ) -> Option<TcpStream> {
        loop {
            // Чекаємо timeout перед спробою
            tokio::select! {
                _ = tokio::time::sleep(timeout) => {}
                _ = shutdown_rx.recv() => {
                    return None; // Shutdown під час очікування
                }
            }
            // Пробуємо підключитись
            tokio::select! {
                result = Self::connect_with_retry(addr, state_tx) => {
                    if let Some(s) = result {
                        return Some(s); // Успіх!
                    }
                    // Не вдалось - продовжуємо loop
                }
                _ = shutdown_rx.recv() => {
                    return None; // Shutdown під час connect
                }
            }
        }
    }

    // Helper: перевірка сокету
    async fn check_socket_alive(stream: &TcpStream) -> bool {
        match tokio::time::timeout(
            Duration::from_millis(100),
            stream.ready(Interest::READABLE | Interest::WRITABLE)
        ).await {
            Ok(Ok(ready)) => !ready.is_read_closed(),
            _ => true, // Timeout = все OK
        }
    }

    // Helper: підключення
    async fn connect_with_retry(
        addr: SocketAddr,
        state_tx: &mpsc::Sender<SocketStateChange>,
    ) -> Option<TcpStream> {
        let _ = state_tx.send(SocketStateChange::Connecting).await;

        match TcpStream::connect(addr).await {
            Ok(stream) => {
                let _ = state_tx.send(SocketStateChange::Connected).await;
                Some(stream)
            }
            Err(e) => {
                let _ = state_tx.send(SocketStateChange::Disconnected).await;
                None
            }
        }
    }

    pub async fn send_read_request(&self, unit: &mut ModbusTCPUnit) -> Result<Vec<i32>, ModbusTCPError> {
        let msg = match unit.create_read_request() {
            Ok(msg) => msg,
            Err(e) => return Err(ModbusTCPError::ModbusUnitError(e))
        };
        let response = self.send_request(msg).await?;
        match unit.parse_response(response) {
            Ok(()) => Ok(unit.get()),
            Err(e) => Err(ModbusTCPError::ModbusUnitError(e))
        }
    }

    async fn send_write_request(&self, unit: &mut ModbusTCPUnit) -> Result<(), ModbusTCPError> {
        let msg = match unit.create_write_request() {
            Ok(msg) => msg,
            Err(e) => return Err(ModbusTCPError::ModbusUnitError(e))
        };
        let response = self.send_request(msg).await?;
        match unit.parse_response(response) {
            Ok(()) => Ok(()),
            Err(e) => Err(ModbusTCPError::ModbusUnitError(e))
        }
    }

    async fn send_request(&self, msg: Vec<u8>) -> Result<Vec<u8>, ModbusTCPError> {
        let (tx, rx) = oneshot::channel();
        let cmd = ModbusCommand {
            packet: msg,
            reply: tx
        };
        self.cmd_tx.borrow_mut()
            .as_mut()
            .ok_or(ModbusTCPError::NotConnected)?
            .send(cmd).await
            .map_err(|_| ModbusTCPError::ModbusClientError("Transport error".to_string()))?;
        let res = match rx.await {
            Ok(response) => match response {
                Ok(resp) => Ok(resp),
                Err(e) => Err(e)
            },
            Err(e) => Err(ModbusTCPError::ModbusClientError("Transport error".to_string()))
        };
        res
    }
}

pub struct ModbusTCPClientBuilder {
    ip: Option<String>,
    port: Option<u16>,
    reconnect_timeout: Option<usize>,
    retry_timeout: Option<Duration>,
    retry_count: Option<usize>,
}

impl ModbusTCPClientBuilder {
    pub fn new() -> Self {
        Self {
            ip: None,
            port: None,
            reconnect_timeout: None,
            retry_count: None,
            retry_timeout: None,
        }
    }

    pub fn ip(&mut self, ip: &str) -> &mut Self {
        if let Ok(parsed) = ip.parse::<Ipv4Addr>() {
            self.ip = Some(parsed.to_string());
        }
        self
    }

    pub fn port<P>(&mut self, port: P) -> &mut Self
    where
        P: TryInto<u16>,
    {
        if let Ok(p) = port.try_into() {
            self.port = Some(p);
        }
        self
    }

    pub fn reconnect_timeout(&mut self, seconds: usize) -> &mut Self {
        self.reconnect_timeout = Some(seconds);
        self
    }

    pub fn retry_timeout(&mut self, mili_seconds: u64) -> &mut Self {
        self.retry_timeout = Some(Duration::from_millis(mili_seconds));
        self
    }

    pub fn retry_count(&mut self, seconds: usize) -> &mut Self {
        self.retry_count = Some(seconds);
        self
    }

    pub fn build(self) -> Result<ModbusTCPClient, ModbusTCPError> {
        let ip = self.ip.ok_or(ModbusTCPError::InvalidIpAddr)?;
        let port = self.port.ok_or(ModbusTCPError::InvalidPort)?;

        let socket_addr = format!("{}:{}", ip, port)
            .parse::<SocketAddr>()
            .map_err(|_| ModbusTCPError::InvalidIpAddr)?;

        let reconnect_timeout = self.reconnect_timeout
            .map(|secs| Duration::from_secs(secs as u64));

        Ok(ModbusTCPClient {
            socket_addr,
            reconnect_timeout,
            state_rx: RefCell::new(None),
            worker_handle: RefCell::new(None),
            shutdown_tx: RefCell::new(None),
            retry_count: self.retry_count.unwrap_or(1),
            retry_interval: self.retry_timeout.unwrap_or(Duration::from_millis(100)),
            cmd_tx: RefCell::new(None),
        })
    }
}
use crate::engine::KvsEngine;
use std::net::{ToSocketAddrs, TcpListener, TcpStream};
use crate::{Result, SharedQueueThreadPool, ThreadPool};
use log::{error, debug};
use std::io::{BufReader, BufWriter};
use serde_json::Deserializer;
use crate::common::{Request, GetResponse, SetResponse, RemoveResponse};
use std::io::Write;


pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        KvsServer {engine}
    }

    pub fn run<A: ToSocketAddrs>(&self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        let thread_pool = SharedQueueThreadPool::new(4)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let engine = self.engine.clone();
                    thread_pool.spawn(|| {
                        if let Err(e) = serve(engine, stream) {
                            error!("Error on serving client: {}", e);
                        }
                    });
               },
                Err(e) => error!("Connection failed: {}", e),
            }
        }
        Ok(())
    }


}

fn serve<E: KvsEngine>(engine: E, tcp: TcpStream) -> Result<()> {
    let peer_addr = tcp.peer_addr()?;
    let reader = BufReader::new(&tcp);
    let mut writer = BufWriter::new(&tcp);
    let req_reader = Deserializer::from_reader(reader).into_iter::<Request>();

    macro_rules! send_resp {
            ($resp:expr) => {{
                let resp = $resp;
                serde_json::to_writer(&mut writer, &resp)?;
                writer.flush()?;
                debug!("Response send {}: {:?}", peer_addr, resp);
            };};
        }

    for req in req_reader {
        let req = req?;
        debug!("Receive request from {}: {:?}", peer_addr, req);
        match req {
            Request::Get {key} => send_resp!(match engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(format!("{}",e))
                 }),
            Request::Set {key, value} => send_resp!(match engine.set(key, value) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(format!("{}",e))
                 }),
            Request::Remove {key} => send_resp!(match engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(format!("{}",e))
                 })
        }
    }
    Ok(())

}
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::net::{TcpStream, ToSocketAddrs};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use kafka_proto::markers::{Request, Response};
use kafka_proto::readable_writable::{KafkaReadable, KafkaWritable};
use kafka_proto::schema::api_versions_request::v0::ApiVersionsRequest;
use kafka_proto::schema::api_versions_response::v0::ApiVersionsResponse;
use kafka_proto::schema::request_header::v1::RequestHeader;
use kafka_proto::schema::response_header::v0::ResponseHeader;

struct Connection {
    stream: TcpStream,
    client_id: Option<String>,
    correlation_id: i32,
}

impl Connection {
    fn new<A: ToSocketAddrs>(addr: A) -> Self {
        Connection {
            stream: TcpStream::connect(addr).unwrap(),
            client_id: Some(String::from("test_client")),
            correlation_id: 0,
        }
    }

    fn send_request<TR: Response>(&mut self, request_api_key: i16, request_api_version: i16, request: impl Request) -> TR {
        let mut cur: Cursor<Vec<u8>> = Cursor::<Vec<u8>>::new(Vec::new());

        cur.write_i32::<BigEndian>(0).unwrap(); // size placeholder

        let header = RequestHeader {
            request_api_key,
            request_api_version,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        };
        self.correlation_id += 1;

        header.write(&mut cur).unwrap();

        request.write(&mut cur).unwrap();

        // Write the real size on top of the placeholder.
        let size = (cur.position() - 4) as i32;
        cur.seek(SeekFrom::Start(0)).unwrap();
        cur.write_i32::<BigEndian>(size).unwrap();

        self.stream.write(cur.get_ref()).unwrap();
        self.stream.flush().unwrap();

        let response_size = self.stream.read_i32::<BigEndian>().unwrap() as usize;
        let mut response_buf = vec![0; response_size];
        let read_size = self.stream.read(&mut response_buf).unwrap();
        assert_eq!(read_size, response_size);

        let mut response_cur = Cursor::new(response_buf);
        let response_header = ResponseHeader::read(&mut response_cur).unwrap();
        assert_eq!(response_header, ResponseHeader::new(self.correlation_id - 1));
        TR::read(&mut response_cur).unwrap()
    }
}

#[test]
fn test_x() {
    let mut connection = Connection::new("127.0.0.1:9092");
    let response: ApiVersionsResponse = connection.send_request(18, 0, ApiVersionsRequest::new());
    println!("{:?}", response);
}

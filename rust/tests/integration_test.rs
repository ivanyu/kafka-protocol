use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::net::{TcpStream, ToSocketAddrs};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use kafka_proto::api_message_type::ApiMessageType;
use kafka_proto::markers::{Request, Response};
use kafka_proto::readable_writable::{KafkaReadable, KafkaWritable};
use kafka_proto::schema::api_versions_request::v0::ApiVersionsRequest;
use kafka_proto::schema::api_versions_response::v0::ApiVersionsResponse;

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

    fn send_request<TReq, TResp>(&mut self,
                                 request_api_key: i16,
                                 request_api_version: i16,
                                 api_message_type: ApiMessageType,
                                 request: TReq) -> TResp where
        TReq: Request + KafkaWritable,
        TResp: Response + KafkaReadable,
    {
        let mut cur: Cursor<Vec<u8>> = Cursor::<Vec<u8>>::new(Vec::new());

        cur.write_i32::<BigEndian>(0).unwrap(); // size placeholder

        match api_message_type.request_header_version(request_api_version) {
            0 => {
                let header = kafka_proto::schema::request_header::v0::RequestHeader::new(
                    request_api_key,
                    request_api_version,
                    self.correlation_id,
                );
                header.write(&mut cur).unwrap();
            },

            1 => {
                let header = kafka_proto::schema::request_header::v1::RequestHeader::new(
                    request_api_key,
                    request_api_version,
                    self.correlation_id,
                    self.client_id.clone(),
                );
                header.write(&mut cur).unwrap();
            },

            2 => {
                let header = kafka_proto::schema::request_header::v2::RequestHeader::new(
                    request_api_key,
                    request_api_version,
                    self.correlation_id,
                    self.client_id.clone(),
                );
                header.write(&mut cur).unwrap();
            },

            v => panic!("Unexpected version {v}")
        };
        self.correlation_id += 1;

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
        let resp_correlation_id = match api_message_type.response_header_version(request_api_version) {
            0 =>
                kafka_proto::schema::response_header::v0::ResponseHeader::read(&mut response_cur).unwrap().correlation_id,

            1 =>
                kafka_proto::schema::response_header::v1::ResponseHeader::read(&mut response_cur).unwrap().correlation_id,

            v => panic!("Unexpected version {v}")
        };
        assert_eq!(resp_correlation_id, self.correlation_id - 1);

        TResp::read(&mut response_cur).unwrap()
    }
}

#[test]
fn test_x() {
    let mut connection = Connection::new("127.0.0.1:9092");
    let response: ApiVersionsResponse = connection.send_request(ApiMessageType::API_VERSIONS.api_key,
                                                                0,
                                                                ApiMessageType::API_VERSIONS,
                                                                ApiVersionsRequest::new());
    println!("{:?}", response);
}

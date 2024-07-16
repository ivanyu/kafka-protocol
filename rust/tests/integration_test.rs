use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::net::{TcpStream, ToSocketAddrs};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use kafka_proto::api_message_type::ApiMessageType;
use kafka_proto::markers::{Request, Response};
use kafka_proto::readable_writable::{KafkaReadable, KafkaWritable};
use paste::paste;

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
                                 api_message_type: ApiMessageType,
                                 request_api_version: i16,
                                 request: TReq) -> TResp where
        TReq: Request + KafkaWritable,
        TResp: Response + KafkaReadable,
    {
        let mut cur: Cursor<Vec<u8>> = Cursor::<Vec<u8>>::new(Vec::new());

        cur.write_i32::<BigEndian>(0).unwrap(); // size placeholder

        match api_message_type.request_header_version(request_api_version) {
            0 => {
                let header = kafka_proto::schema::request_header::v0::RequestHeader::new(
                    api_message_type.api_key,
                    request_api_version,
                    self.correlation_id,
                );
                header.write(&mut cur).unwrap();
            }

            1 => {
                let header = kafka_proto::schema::request_header::v1::RequestHeader::new(
                    api_message_type.api_key,
                    request_api_version,
                    self.correlation_id,
                    self.client_id.clone(),
                );
                header.write(&mut cur).unwrap();
            }

            2 => {
                let header = kafka_proto::schema::request_header::v2::RequestHeader::new(
                    api_message_type.api_key,
                    request_api_version,
                    self.correlation_id,
                    self.client_id.clone(),
                );
                header.write(&mut cur).unwrap();
            }

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

macro_rules! test_api_versions {
    ($conn: ident, $api_message_type: expr, $version: literal $(,$arg: expr)*) => {
        paste! {
            println!("{}", $($api_message_type.name):lower);

            let response: kafka_proto::schema::api_versions_response::[<v $version>]::ApiVersionsResponse = $conn.send_request(
                $api_message_type,
                $version,
                kafka_proto::schema::api_versions_request::[<v $version>]::ApiVersionsRequest::new($($arg),*)
            );
        }
        println!("{response:?}");
        assert_eq!(response.error_code, 0);
        assert!(response.api_keys.len() > 0);
    }
}

#[test]
fn test_x() {
    let mut connection = Connection::new("127.0.0.1:9092");
    test_api_versions!(connection, ApiMessageType::API_VERSIONS, 0);
    test_api_versions!(connection, ApiMessageType::API_VERSIONS, 1);
    test_api_versions!(connection, ApiMessageType::API_VERSIONS, 2);
    // test_api_versions!(connection, ApiMessageType::API_VERSIONS, 3, "client_software_name".to_string(), "client_software_version".to_string());
}

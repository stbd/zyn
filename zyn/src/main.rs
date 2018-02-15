extern crate zyn;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate chrono;

use std::env::{ args };
use std::path::{ PathBuf };
use std::process::{ exit };
use std::str::{ FromStr };
use std::string::{ String } ;
use std::vec::{ Vec };

use zyn::node::node::{ Node, NodeSettings };
use zyn::node::common::{ ADMIN_GROUP, ADMIN_GROUP_NAME, utc_timestamp };
use zyn::node::connection::{ Server };
use zyn::node::crypto::{ Crypto };
use zyn::node::user_authority::{ UserAuthority };

const EXIT_STATUS_OK: i32 = 0;
const EXIT_STATUS_ERROR: i32 = 1;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[derive(Debug)]
enum Argument {
    String { value: Option<String> },
    Path { value: Option<PathBuf> },
    Bool { value: Option<bool> },
    Uint { value: Option<u64> },
}

impl Argument {
    pub fn take_string(& mut self) -> String {
        match *self {
            Argument::String{ ref mut value } => value.take().unwrap(),
            _ => panic!(),
        }
    }

    pub fn take_uint(& mut self) -> u64 {
        match *self {
            Argument::Uint{ ref mut value } => value.take().unwrap(),
            _ => panic!(),
        }
    }

    pub fn take_bool(& mut self) -> bool {
        match *self {
            Argument::Bool{ ref mut value } => value.take().unwrap(),
            _ => panic!(),
        }
    }

    pub fn take_path(& mut self) -> PathBuf {
        match *self {
            Argument::Path{ ref mut value } => value.take().unwrap(),
            _ => panic!(),
        }
    }
}

type ParamItem = (& 'static str, & 'static str, Argument);

#[derive(Debug)]
struct Arguments {
    values: Vec<ParamItem>,
}

impl Arguments {
    fn name_gpg_fingerprint() -> & 'static str {
        "--gpg-fingerprint"
    }

    fn name_path_cert() -> & 'static str {
        "--path-cert"
    }

    fn name_path_key() -> & 'static str {
        "--path-key"
    }

    fn name_local_address() -> & 'static str {
        "--local-address"
    }

    fn name_local_port() -> & 'static str {
        "--local-port"
    }

    pub fn name_data_dir() -> & 'static str {
        "--path-data-dir"
    }

    pub fn name_init() -> & 'static str {
        "--init"
    }

    pub fn default_user_name() -> & 'static str {
        "--default-user-name"
    }

    pub fn default_user_password() -> & 'static str {
        "--default-user-password"
    }

    pub fn defaults() -> Arguments {
        Arguments {
            values: vec![
                (Arguments::name_gpg_fingerprint(),
                 "Fingerprint of a GPG key used to encrypt/decrypt data on disk",
                 Argument::String { value: None }),

                (Arguments::name_path_cert(),
                 "Path to certicate used TLS communication",
                 Argument::Path { value: None }),

                (Arguments::name_path_key(),
                 "Path to key used in TLS communicaiton",
                 Argument::Path { value: None }),

                (Arguments::name_local_address(),
                 "Local IP to which server socket is bind",
                 Argument::String { value: Some(String::from("127.0.0.1")) }),

                (Arguments::name_local_port(),
                 "Local port to which server socket is bind",
                 Argument::Uint { value: Some(8080) }),

                (Arguments::name_data_dir(),
                 "Path to directory used to store presistent data",
                 Argument::Path { value: None }),

                (Arguments::name_init(),
                 "Initialize data directory",
                 Argument::Bool { value: Some(false) }),

                (Arguments::default_user_name(),
                 "Name of the default user",
                 Argument::String { value: Some(String::from("admin")) }),

                (Arguments::default_user_password(),
                 "Password of the default user",
                 Argument::String { value: Some(String::from("admin")) }),

            ],
        }
    }

    pub fn is_empty(& self) -> bool {
        self.values.is_empty()
    }

    pub fn take(& mut self, name: & str) -> Argument {
        let mut index: Option<usize> = None;
        for (i, & (ref param_name, _, _)) in self.values.iter().enumerate() {
            if *param_name == name {
                index = Some(i);
                break;
            }
        }

        if let Some(i) = index {
            return self.values.swap_remove(i).2;
        }
        panic!(format!("{} not found", name));
    }

    fn validate(& self) {
        let not_set = | name: & str | println!("Argument {} is required but was not specified", name);
        let mut ok = true;

        for & (ref name, _, ref param) in self.values.iter() {
            match param {
                & Argument::Uint { ref value } => {
                    if value.is_none() {
                        not_set(& name);
                        ok = false;
                    }
                },
                & Argument::String { ref value } => {
                    if value.is_none() {
                        not_set(& name);
                        ok = false;
                    }
                },
                & Argument::Path { ref value } => {
                    if let & Some(ref v) = value {
                        if !v.exists() {
                            println!("Path does not exist: path={}", v.display());
                            ok = false;
                        }
                    } else {
                        not_set(& name);
                        ok = false;
                    }
                },
                & Argument::Bool { ref value } => {
                    if value.is_none() {
                        not_set(& name);
                        ok = false;
                    }
                }
            }
        }
        if !ok {
            println!("Error parsing arguments, use --help to print help text");
            exit(EXIT_STATUS_ERROR);
        }
    }

    pub fn usage(& self) {

        println!();
        println!("\tZyn {}", VERSION);
        println!();
        println!("Usage: zyn [arguments]");
        println!();
        println!("Possible arguments:");
        println!(" -h/--help - Print help, this printout");
        for & (ref name, ref desc, _) in self.values.iter() {
            println!(" {} - {}", name, desc);
        }

    }

    fn parse_arguments(& mut self) {
        let mut index: usize = 1;
        let args: Vec<String> = args().collect();

        while index < args.len() {
            let ref arg = args[index];

            if arg == "-h" || arg == "--help" {
                self.usage();
                exit(EXIT_STATUS_ERROR);
            }

            let next;
            if (index + 1) < args.len() {
                next = Some(& args[index + 1]);
            } else {
                next = None;
            }

            let mut found: bool = false;
            for & mut (ref name, _, ref mut param) in self.values.iter_mut() {
                if name != arg {
                    continue;
                }

                found = true;
                let ok = match param {
                    & mut Argument::String { ref mut value } => {
                        if let Some(v) = next {
                            *value = Some(v.clone());
                            index += 1;
                            true
                        } else {
                            false
                        }
                    },
                    & mut Argument::Uint { ref mut value } => {
                        if let Some(v) = next {
                            *value = Some(u64::from_str(v).expect(& format!("Failed to parse \"{}\" as unsigned integer", v)));
                            index += 1;
                            true
                        } else {
                            false
                        }
                    },
                    & mut Argument::Path { ref mut value } => {
                        if let Some(v) = next {
                            *value = Some(PathBuf::from(v));
                            index += 1;
                            true
                        } else {
                            false
                        }
                    },
                    & mut Argument::Bool { ref mut value } => {
                        *value = Some(true);
                        true
                    },
                };

                if !ok {
                    println!("Failed to parse value for {}", name);
                    exit(EXIT_STATUS_ERROR);
                }
            }

            if !found {
                println!("Unknown argument: {}", arg);
                exit(EXIT_STATUS_ERROR);
            }
            index += 1;
        }
        self.validate();
    }
}

fn run() -> Result<(), ()> {
    let mut args = Arguments::defaults();
    args.parse_arguments();
    trace!("Application started with arguments: {:?}", args);

    let data_dir = args.take(Arguments::name_data_dir()).take_path();

    let server = Server::new(
        & args.take(Arguments::name_local_address()).take_string(),
        args.take(Arguments::name_local_port()).take_uint() as u16,
        & args.take(Arguments::name_path_key()).take_path(),
        & args.take(Arguments::name_path_cert()).take_path())
        .map_err(| () | error!("Failed to init TCP server"))
        ? ;


    let gpg_fingerprint = args.take(Arguments::name_gpg_fingerprint()).take_string();

    let username = args.take(Arguments::default_user_name()).take_string();
    let password = args.take(Arguments::default_user_password()).take_string();
    let create = args.take(Arguments::name_init()).take_bool();

    if ! args.is_empty() {
        panic!("Unused arguments");
    }

    let crypto = Crypto::new(gpg_fingerprint)
        .map_err(| () | error!("Failed to init crypto"))
        ? ;

    if create {
        const MEGABYTE: usize = 1024 * 1024;
        let node_settings = NodeSettings {
            page_size_random_access_file: MEGABYTE * 5,
            page_size_blob_file: MEGABYTE * 10,
            socket_buffer_size: 1024 * 4,
        };

        let mut user_authority = UserAuthority::new("fixme");
        user_authority.configure_admin_group(& ADMIN_GROUP, ADMIN_GROUP_NAME)
            .map_err(| () | error!("Failed to configure admin group"))
            ? ;

        let default_user_expiration = utc_timestamp() + 5 * 60;
        let user = user_authority.add_user(& username, & password, Some(default_user_expiration))
            .map_err(| () | error!("Failed to create default user"))
            ? ;

        user_authority.modify_group_add_user(& ADMIN_GROUP, & user, utc_timestamp())
            .map_err(| () | error!("Failed to add default user to admin group"))
            ? ;

        Node::create(
            crypto.clone(),
            user_authority,
            & data_dir,
            node_settings,
        ).map_err(| () | error!("Failed to initialize node"))
            ? ;
    }

    let mut node = Node::load(crypto, server, & data_dir)
        .map_err(| () | error!("Failed to load node"))
        ? ;

    node.run()
}

fn main() {
    match env_logger::init() {
        Err(desc) => println!("Failed to init logging: {}", desc),
        Ok(()) => (),
    };

    match run() {
        Ok(()) => exit(EXIT_STATUS_OK),
        Err(()) => exit(EXIT_STATUS_ERROR),
    };
}

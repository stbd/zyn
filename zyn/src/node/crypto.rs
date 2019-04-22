use std::io::{ BufWriter, Write };
use std::fs::{ remove_file };
use std::option::{ Option };
use std::path::{ Path };
use std::process::{ Command, Stdio, Output };
use std::result::{ Result };
use std::string::{ String };
use std::vec::{ Vec };

#[derive(Clone)]
pub struct Crypto {
    fingerprint: String
}

impl Crypto {
    pub fn new(fingerprint: String) -> Result<Crypto, ()> {

        Ok(Crypto {
            fingerprint: fingerprint,
        })
    }

    pub fn create_context(& self) -> Result<Context, ()> {

        Ok(Context {
            fingerprint: self.fingerprint.clone(),
        })
    }
}

unsafe impl Send for Context {}
unsafe impl Sync for Context {}
pub struct Context {
    fingerprint: String
}

impl Context {
    fn run_command(& self, command: & mut Command, input_data: Option<&[u8]>)
                   -> Result<Output, ()> {

        let mut process = command.spawn()
            .map_err(| error | {
                error!("Failed to run GPG command, error=\"{}\"", error);
            })
            ? ;

        match input_data {
            Some(ref data) => {
                match & mut process.stdin {
                    & mut Some(ref mut stdin) => {
                        let mut writer = BufWriter::new(stdin);
                        writer.write_all(data)
                            .map_err(| error | {
                                error!("Failed to write to GPG process stdin, error=\"{}\"",
                                       error
                                );
                            })
                            ? ;
                    },
                    & mut None => {
                        error!("GPG process has no stdin");
                        return Err(())
                    },
                };
            },
            None => (),
        };

        match process.wait_with_output() {
            Ok(output) => Ok(output),
            Err(error) => {
                error!("GPG process failed to complete, error=\"{}\"", error);
                Err(())
            },
        }
    }

    fn gpg_command_base(& self) -> Command {
        let mut cmd = Command::new("gpg2");
        cmd
            .arg("--no-tty")
            .arg("--batch");

        cmd
    }

    pub fn decrypt_from_file(& self, path_input: & Path)
                             -> Result<Vec<u8>, ()>
    {

        let output = self.run_command(
            self.gpg_command_base()
                .arg("--decrypt")
                .arg(path_input.to_str().unwrap())
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .stdout(Stdio::piped()),
            None,
        )
            ? ;

        if ! output.status.success() {
            match output.status.code() {
                Some(code) => {
                    error!("GPG decrypt process failed, error_code={}", code);
                },
                None => {
                    error!("GPG decrypt process failed without error");
                },
            }
            return Err(());
        }

        trace!("Succefully decrypted {} ciphertext bytes from file, path=\"{}\"",
               output.stdout.len(), path_input.display());

        Ok(output.stdout)
    }

    pub fn encrypt_to_file(& self, plaintext: & [u8], path_output: & Path)
                           -> Result<(), ()>
    {

        let _ = remove_file(& path_output);

        let plaintext_length = plaintext.len();
        let output = self.run_command(
            self.gpg_command_base()
                .arg("--encrypt")
                .arg("-r")
                .arg(self.fingerprint.clone())
                .arg("--output")
                .arg(path_output.to_str().unwrap())
                .stdin(Stdio::piped())
                .stderr(Stdio::null())
                .stdout(Stdio::null()),
            Some(plaintext),
        )
            ? ;

        if ! output.status.success() {
            match output.status.code() {
                Some(code) => {
                    error!("GPG encrypt process failed, error_code={}", code);
                },
                None => {
                    error!("GPG encrypt process failed without error");
                },
            }
            return Err(());
        }

        trace!("Succefully encrypted {} bytes of plaintext into file, path=\"{}\"",
               plaintext_length, path_output.display());

        Ok(())
    }
}

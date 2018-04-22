use std::io::{ BufWriter, Write };
use std::process::{ Command, Stdio, Output };
use std::string::{ String };
use std::result::{ Result };
use std::vec::{ Vec };

pub struct CryptoGpg {
    fingerprint: String
}

impl CryptoGpg {
    pub fn new(fingerprint: String) -> Result<CryptoGpg, ()> {

        Ok(CryptoGpg {
            fingerprint: fingerprint,
        })
    }

    pub fn create_context(& self) -> Result<ContextGpg, ()> {

        Ok(ContextGpg {
            fingerprint: self.fingerprint.clone(),
        })
    }
}

pub struct ContextGpg {
    fingerprint: String
}

impl ContextGpg {
    fn run_command(& self, command: & mut Command, input_data: &[u8])
                   -> Result<Output, ()> {

        let mut process = command.spawn()
            .map_err(| error | {
                error!("Failed to run GPG command, error=\"{}\"", error);
            })
            ? ;

        match & mut process.stdin {
            & mut Some(ref mut stdin) => {
                let mut writer = BufWriter::new(stdin);
                writer.write_all(input_data)
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

        match process.wait_with_output() {
            Ok(output) => Ok(output),
            Err(error) => {
                error!("GPG process failed to complete, error=\"{}\"", error);
                Err(())
            },
        }
    }


    pub fn decrypt(& self, ciphertext: & [u8]) -> Result<Vec<u8>, ()> {

        let ciphertext_length = ciphertext.len();
        let output = self.run_command(
            Command::new("gpg2")
                .arg("--no-tty")
                .arg("--batch")
                .arg("--decrypt")
                .stdin(Stdio::piped())
                .stderr(Stdio::null())
                .stdout(Stdio::piped()),
            ciphertext
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

        trace!("Succefully decrypted {} ciphertext bytes into {} bytes",
               ciphertext_length, output.stdout.len());

        Ok(output.stdout)
    }

    pub fn decrypt_into_string(& self, ciphertext: & [u8]) -> Result<String, ()> {
        let decrypted = self.decrypt(ciphertext)
            ? ;
        Ok(String::from_utf8_lossy(& decrypted).into_owned())
    }

    pub fn encrypt(& self, plaintext: & [u8]) -> Result<Vec<u8>, ()> {

        let plaintext_length = plaintext.len();
        let output = self.run_command(
            Command::new("gpg2")
                .arg("--batch")
                .arg("--no-tty")
                .arg("--encrypt")
                .arg("-r")
                .arg(self.fingerprint.clone())
                .stdin(Stdio::piped())
                .stderr(Stdio::null())
                .stdout(Stdio::piped()),
            plaintext,
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

        trace!("Succefully encrypted {} bytes of plaintext into {} bytes",
               plaintext_length, output.stdout.len());

        Ok(output.stdout)
    }
}

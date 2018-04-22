use node::test_util;

#[test]
fn test_encrypt_decrypt() {
    let context = test_util::create_crypto_context_gpg();
    let plaintext = String::from("data");
    let ciphertext = context.encrypt(plaintext.as_bytes()).unwrap();
    let decrypted = context.decrypt_into_string(& ciphertext).unwrap();
    assert!(plaintext == decrypted);
}

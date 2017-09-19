use node::test_util;

#[test]
fn test_encrypt_decrypt() {
    test_util::init_logging();
    let context = test_util::create_crypto_context();
    let data_original: Vec<u8> = vec![1, 2, 3, 4, 5];
    let data_encrypted = context.encrypt(& data_original).unwrap();
    let data_decrypted = context.decrypt(& data_encrypted).unwrap();
    assert!(data_original != data_encrypted);
    assert!(data_original == data_decrypted);
}

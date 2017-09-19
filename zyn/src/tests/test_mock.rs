use std::hash::{ Hash, Hasher };
use std::collections::hash_map::{ DefaultHasher };
use std::fmt;

const MAX_NUMBER_OF_PARAMETERS: usize = 10;
const MAX_NUMBER_OF_FUNCTION_CALLS: usize = 10;

#[derive(Clone, Copy, PartialEq)]
pub enum Parameter {
    ValueI32 { value: i32 },
    ValueF32 { value: f32 },
    NotSet,
}

#[allow(non_snake_case)]
pub fn P_I32(value: i32) -> Parameter {
    return Parameter::ValueI32{ value: value }
}
#[allow(non_snake_case)]
pub fn P_F32(value: f32) -> Parameter {
    return Parameter::ValueF32{ value: value }
}

#[derive(Clone, Copy)]
pub struct Parameters {
    current_index: usize,
    params: [Parameter; MAX_NUMBER_OF_PARAMETERS],
}

impl Parameters {
    pub fn new() -> Parameters {
        Parameters {
            params: [Parameter::NotSet; MAX_NUMBER_OF_PARAMETERS],
            current_index: 0
        }
    }
    pub fn add(& mut self, parameter: Parameter) {
        if self.current_index == MAX_NUMBER_OF_PARAMETERS {
            panic!("Number of parameters exceeds limit");
        }
        self.params[self.current_index] = parameter;
        self.current_index += 1;
    }
}

impl fmt::Debug for Parameters {
    fn fmt(& self, f: & mut fmt::Formatter) -> fmt::Result {
        let mut ret = writeln!(f, "Parameters ({})", self.current_index);
        for param in & self.params {
            match param {
                &Parameter::ValueI32{value} => {
                    ret = writeln!(f, "\t{} (i32)", value);
                },
                &Parameter::ValueF32{value} => {
                    ret = writeln!(f, "\t{} (f32)", value);
                },
                &Parameter::NotSet => break,
            }
        }
        ret
    }
}

impl PartialEq for Parameters {
    fn eq (& self, other: & Parameters) -> bool {
        if self.current_index != other.current_index {
            return false
        }
        for i in 0..MAX_NUMBER_OF_PARAMETERS {
            if self.params[i] != other.params[i] {
                return false
            }
        }
        true
    }
}

#[derive(Clone, Copy)]
struct FunctionCall {
    pub class_name: u64,
    pub function_name: u64,
    pub parameters: Parameters,
}

impl FunctionCall {
    pub fn set(
        & mut self,
        class_name: u64,
        function_name: u64,
        parameters: Parameters) -> bool {

        self.class_name = class_name;
        self.function_name = function_name;
        self.parameters = parameters;
        true
    }
}

fn hash(string: & str) -> u64 {
    let mut hasher = DefaultHasher::new();
    string.hash(& mut hasher);
    hasher.finish()
}


pub struct FunctionCallRegister {
    call_index: usize,
    function_calls: [FunctionCall; MAX_NUMBER_OF_FUNCTION_CALLS],
}

impl FunctionCallRegister {
    pub fn clear(& mut self) {
        self.call_index = 0;
    }

    pub fn validate_function_call(
        & mut self,
        call_number: usize,
        class_name: & str,
        function_name: & str,
        expected_parameters: Parameters,
        panic: bool,
    ) -> bool {
        let function_call = self.function_calls[call_number];

        let equal_class = function_call.class_name == hash(class_name);
        if equal_class == false {
            println!(
                "Expected class name \"{}\" does not match at call index {}",
                class_name,
                call_number
            );
            if panic {
                panic!();
            }
            return false
        }

        let equal_function = function_call.function_name == hash(function_name);
        if equal_function == false {
            println!(
                "Expected method name \"{}\" does not match at call index {}",
                class_name,
                call_number
            );
            if panic {
                panic!();
            }
            return false
        }

        let equal_params = function_call.parameters == expected_parameters;
        if equal_params == false {
            println!("Expected parameters do not match at call index {}", call_number);
            println!("Received parameters:");
            println!("{:?}", function_call.parameters);
            println!("Expected parameters:");
            println!("{:?}", expected_parameters);
            if panic {
                panic!();
            }
            return false
        }
        true
    }

    pub fn register_function_call(
        & mut self,
        class_name: & str,
        function_name: & str,
        parameters: Parameters
    ) {
        if self.call_index == MAX_NUMBER_OF_FUNCTION_CALLS {
            panic!("Number of function calls exceeds max limit");
        }

        self.function_calls[self.call_index].set(
            hash(class_name),
            hash(function_name),
            parameters,
        );
        self.call_index += 1;
    }

    pub fn current_function_call_index(& self) -> usize{
        self.call_index
    }
}

pub static mut FUNCTION_CALL_REGISTER: FunctionCallRegister = FunctionCallRegister {
    call_index: 0,
    function_calls: [
        FunctionCall {
            class_name: 0,
            function_name: 0,
            parameters: Parameters {
                params: [Parameter::NotSet; MAX_NUMBER_OF_PARAMETERS],
                current_index: 0
            }
        }; MAX_NUMBER_OF_FUNCTION_CALLS]
};

#[macro_export]
macro_rules! create_mock_class {
    ($class_name:ident) => {
        struct $class_name {}
        impl $class_name {
            #[allow(dead_code)]
            fn new() -> $class_name {
                unsafe {
                    FUNCTION_CALL_REGISTER.clear();
                }
                $class_name { }
            }

            #[allow(dead_code)]
            fn record_record_function_call(& self, funtion_name: & str, params: Parameters) {
                unsafe {
                    FUNCTION_CALL_REGISTER.register_function_call(stringify! ($class_name), funtion_name, params);
                }
            }

            #[allow(dead_code)]
            pub fn validate_function_call_without_panic(
                & self,
                call_number: usize,
                function_name: & str,
                parameters: Parameters) -> bool {
                unsafe {
                    FUNCTION_CALL_REGISTER.validate_function_call(
                        call_number,
                        stringify! ($class_name),
                        function_name,
                        parameters,
                        false
                    )
                }
            }

            #[allow(dead_code)]
            pub fn validate_function_call(
                & self,
                call_number: usize,
                function_name: & str,
                parameters: Parameters) {
                unsafe {
                    FUNCTION_CALL_REGISTER.validate_function_call(
                        call_number,
                        stringify! ($class_name),
                        function_name,
                        parameters,
                        true,
                    );
                }
            }
        }
    }
}

#[macro_export]
macro_rules! use_mock {
    () => {
        #[allow(unused_imports)]
        use tests::test_mock::{
            FUNCTION_CALL_REGISTER,
            Parameters,
            P_I32,
            P_F32,
        };
    }
}

#[macro_export]
macro_rules! create_parameters {
    ( $($y:expr), *) => {{
        let mut params = Parameters::new();
        $(
            params.add($y);
        )*
        params

    }}
}


create_mock_class!(MockedClass);
impl MockedClass {
    #[allow(dead_code)]
    fn write(& self, value: i32) -> bool {
        let mut ret = false;
        unsafe {
            if FUNCTION_CALL_REGISTER.current_function_call_index() == 0 {
                ret = true;
            }
        }
        self.record_record_function_call(
            "write",
            create_parameters!(P_I32(value))
        );
        ret
    }
}

#[test]
fn test_mock() {
    let mocked = MockedClass::new();
    let val_1: i32 = 33;
    let val_2: i32 = 37;
    assert!(mocked.write(val_1) == true);
    assert!(mocked.write(val_2) == false);
    assert!(
        mocked.validate_function_call_without_panic(
            0, "write", create_parameters!(P_I32(val_1))
        ) == true);
    assert!(
        mocked.validate_function_call_without_panic(
            1, "write", create_parameters!(P_I32(val_2))
        ) == true);

    assert!(
        mocked.validate_function_call_without_panic(
            1, "write", create_parameters!(P_I32(val_2), P_F32(0.4))
        ) == false);
    assert!(
        mocked.validate_function_call_without_panic(
            1, "write", create_parameters!(P_I32(val_1))
        ) == false);
    assert!(
        mocked.validate_function_call_without_panic(
            0, "write-smt", create_parameters!(P_I32(val_1))
        ) == false);
}

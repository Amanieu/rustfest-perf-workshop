#![cfg_attr(test, feature(test))]

#[macro_use]
extern crate combine;
extern crate fxhash;

use fxhash::FxHashMap;
use std::{fmt, mem};

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Ident(usize);

#[derive(Copy, Clone)]
pub enum Value {
    Void,
    False,
    Int(u64),
    Code(usize),
    InbuiltFunc(fn(&[Value]) -> Value),
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Void => write!(f, "Void"),
            Value::False => write!(f, "False"),
            Value::Int(x) => write!(f, "Int({})", x),
            Value::Code(x) => write!(f, "Code({})", x),
            Value::InbuiltFunc(_) => write!(f, "InbuiltFunc"),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum Opcode {
    Lit(Value),
    ReadVar(Ident),
    WriteVar(Ident),
    Call { num_args: usize, ret_addr: usize },
    Return,
    DropVal,
    End,
}

#[derive(Clone, Debug)]
pub struct Variables<'a> {
    idents: FxHashMap<&'a str, Ident>,
    variables: Vec<Option<(Value, usize)>>,
}
impl<'a> Variables<'a> {
    pub fn new() -> Variables<'a> {
        Variables {
            idents: FxHashMap::default(),
            variables: Vec::new(),
        }
    }
    pub fn ident(&mut self, s: &'a str) -> Ident {
        let new_ident = Ident(self.variables.len());
        let variables = &mut self.variables;
        *self.idents.entry(s).or_insert_with(|| {
            variables.resize(new_ident.0 + 1, Default::default());
            new_ident
        })
    }
    pub fn set_var(&mut self, var: &'a str, val: Value) {
        let ident = self.ident(var);
        self.set(ident, val, 0);
    }

    pub fn set(&mut self, ident: Ident, val: Value, scope: usize) -> Option<(Value, usize)> {
        mem::replace(&mut self.variables[ident.0], Some((val, scope)))
    }

    pub fn get(&self, ident: Ident, scope_depth: usize) -> Value {
        match self.variables.get(ident.0).and_then(|x| *x) {
            Some(v) if v.1 <= scope_depth => v.0,
            _ => panic!("Variable does not exist: {:?}\n{:#?}", ident, self),
        }
    }
}

pub struct CompileContext<'a> {
    code: Vec<Vec<Opcode>>,
    current_block: usize,
    variables: Variables<'a>,
}

impl<'a> CompileContext<'a> {
    fn emit(&mut self, op: Opcode) {
        let block = self.current_block;
        self.code[block].push(op);
    }

    pub fn compile(program: &[Ast<'a>], variables: Variables<'a>) -> RunContext<'a> {
        let mut ctx = CompileContext {
            code: vec![Vec::new()],
            current_block: 0,
            variables,
        };
        ctx.emit(Opcode::Lit(Value::Void));
        for ast in program {
            ctx.emit(Opcode::DropVal);
            ctx.compile_subtree(ast);
        }
        ctx.emit(Opcode::End);
        let code = ctx.optimize();
        RunContext {
            code,
            stack: Vec::new(),
            ret_stack: Vec::new(),
            variables: ctx.variables,
        }
    }

    fn optimize(&mut self) -> Vec<Opcode> {
        // Eliminate literals followed by DropValue
        for block in &mut self.code {
            let mut i = 0;
            while i < block.len() - 1 {
                if let Opcode::Lit(_) = block[i] {
                    if let Opcode::DropVal = block[i + 1] {
                        block.drain(i..i + 2);
                        continue;
                    }
                }
                i += 1;
            }
        }

        // Move all the code into a single Vec
        let mut offsets = Vec::with_capacity(self.code.len());
        let mut pos = 0;
        for block in &self.code {
            offsets.push(pos);
            pos += block.len();
        }
        for block in &mut self.code {
            for op in block.iter_mut() {
                match op {
                    Opcode::Lit(Value::Code(ref mut target)) => *target = offsets[*target],
                    Opcode::Call {
                        ref mut ret_addr, ..
                    } => *ret_addr = offsets[*ret_addr],
                    _ => {}
                }
            }
            offsets.push(pos);
            pos += block.len();
        }
        self.code
            .iter()
            .flat_map(|block| block.iter())
            .map(|x| *x)
            .collect()
    }

    fn compile_subtree(&mut self, tree: &Ast<'a>) {
        match tree {
            Ast::Lit(val) => {
                let val = match val {
                    AstValue::Void => Value::Void,
                    AstValue::False => Value::False,
                    AstValue::Int(x) => Value::Int(*x),
                    AstValue::Function(args, ast) => {
                        let prev_block = self.current_block;
                        let new_block = self.code.len();
                        self.code.push(Vec::new());
                        self.current_block = new_block;

                        for arg in args.into_iter().rev() {
                            let ident = self.variables.ident(arg);
                            self.emit(Opcode::WriteVar(ident));
                        }
                        self.emit(Opcode::Lit(Value::Void));
                        for ast in ast {
                            self.emit(Opcode::DropVal);
                            self.compile_subtree(ast);
                        }
                        self.emit(Opcode::Return);

                        self.current_block = prev_block;
                        Value::Code(new_block)
                    }
                    AstValue::InbuiltFunc(f) => Value::InbuiltFunc(*f),
                };
                self.emit(Opcode::Lit(val));
            }
            Ast::Variable(name) => {
                let ident = self.variables.ident(name);
                self.emit(Opcode::ReadVar(ident));
            }
            Ast::Call(func, arguments) => {
                let ret_addr = self.code.len();
                self.code.push(Vec::new());

                let num_args = arguments.len();
                self.compile_subtree(func);
                for ast in arguments {
                    self.compile_subtree(ast);
                }
                self.emit(Opcode::Call { num_args, ret_addr });

                self.current_block = ret_addr;
            }
            Ast::Define(name, value) => {
                self.compile_subtree(value);
                let ident = self.variables.ident(name);
                self.emit(Opcode::WriteVar(ident));
                self.emit(Opcode::Lit(Value::Void));
            }
        }
    }
}

enum ScopeStack {
    ReturnAddress(usize),
    RestoreIdent(Ident, Value, usize),
}

pub struct RunContext<'a> {
    code: Vec<Opcode>,
    stack: Vec<Value>,
    ret_stack: Vec<ScopeStack>,
    variables: Variables<'a>,
}

impl<'a> RunContext<'a> {
    pub fn run(&mut self) -> Value {
        let mut pc = 0;
        let mut scope_depth = 1;
        loop {
            match self.code[pc] {
                Opcode::Lit(val) => {
                    self.stack.push(val);
                    pc += 1;
                }
                Opcode::ReadVar(ident) => {
                    let val = self.variables.get(ident, scope_depth);
                    self.stack.push(val);
                    pc += 1;
                }
                Opcode::WriteVar(ident) => {
                    let val = self.stack.pop().expect("Operand stack underflow");
                    if let Some((old_val, old_scope)) = self.variables.set(ident, val, scope_depth)
                    {
                        self.ret_stack
                            .push(ScopeStack::RestoreIdent(ident, old_val, old_scope));
                    }
                    pc += 1;
                }
                Opcode::Call { num_args, ret_addr } => {
                    let stack_top = self.stack.len();
                    let args_start = stack_top - num_args;
                    let target = *self.stack
                        .get(args_start - 1)
                        .expect("Operand stack underflow");
                    match target {
                        Value::Code(target) => {
                            self.ret_stack.push(ScopeStack::ReturnAddress(ret_addr));
                            scope_depth += 1;
                            pc = target;
                        }
                        Value::InbuiltFunc(f) => {
                            let result = f(&self.stack[args_start..]);
                            self.stack.truncate(args_start);
                            self.stack[args_start - 1] = result;
                            pc = ret_addr;
                        }
                        _ => panic!("Attempted to call a non-function"),
                    }
                }
                Opcode::Return => {
                    let result = self.stack.pop().expect("Operand stack underflow");
                    *self.stack.last_mut().expect("Operand stack underflow") = result;
                    scope_depth -= 1;
                    let ret_addr = loop {
                        match self.ret_stack.pop().expect("Return stack underflow") {
                            ScopeStack::ReturnAddress(r) => break r,
                            ScopeStack::RestoreIdent(ident, val, depth) => {
                                self.variables.set(ident, val, depth);
                            }
                        }
                    };
                    pc = ret_addr;
                }
                Opcode::DropVal => {
                    self.stack.pop().expect("Operand stack underflow");
                    pc += 1;
                }
                Opcode::End => {
                    while let Some(x) = self.ret_stack.pop() {
                        match x {
                            ScopeStack::ReturnAddress(r) => unreachable!(),
                            ScopeStack::RestoreIdent(ident, val, depth) => {
                                self.variables.set(ident, val, depth);
                            }
                        }
                    }
                    return self.stack.pop().expect("Operand stack underflow");
                }
            }
        }
    }
}

#[derive(Clone)]
pub enum Ast<'a> {
    Lit(AstValue<'a>),
    Variable(&'a str),
    Call(Box<Ast<'a>>, Vec<Ast<'a>>),
    Define(&'a str, Box<Ast<'a>>),
}

#[derive(Clone)]
pub enum AstValue<'a> {
    Void,
    False,
    Int(u64),
    Function(Vec<&'a str>, Vec<Ast<'a>>),
    InbuiltFunc(fn(&[Value]) -> Value),
}
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;

        match (self, other) {
            (&Void, &Void) => true,
            (&False, &False) => true,
            (&Int(a), &Int(b)) => a == b,
            _ => false,
        }
    }
}

/*
pub fn eval<'a>(program: Ast<'a>, variables: &mut HashMap<&'a str, Value<'a>>) -> Value<'a> {
    use self::Ast::*;
    use self::Value::*;

    match program {
        Lit(val) => val,
        Variable(name) => match variables.get(&name) {
            Some(v) => v.clone(),
            _ => panic!("Variable does not exist: {}", name),
        },
        Call(func, arguments) => {
            let func = eval(*func, variables);

            match func {
                Function(args, body) => {
                    // Start a new scope, so all variables defined in the body of the
                    // function don't leak into the surrounding scope.
                    let mut new_scope = variables.clone();

                    if arguments.len() != args.len() {
                        println!("Called function with incorrect number of arguments (expected {}, got {})", args.len(), arguments.len());
                    }

                    for (name, val) in args.into_iter().zip(arguments) {
                        let val = eval(val, variables);
                        new_scope.insert(name, val);
                    }

                    let mut out = Void;

                    for stmt in body {
                        out = eval(stmt, &mut new_scope);
                    }

                    out
                }
                InbuiltFunc(func) => func(
                    arguments
                        .into_iter()
                        .map(|ast| eval(ast, variables))
                        .collect(),
                ),
                _ => panic!("Attempted to call a non-function"),
            }
        }
        Define(name, value) => {
            let value = eval(*value, variables);

            variables.insert(name, value);

            Void
        }
    }
}
*/

parser! {
    pub fn expr['a, I]()(I) -> Ast<'a> where [
        I: combine::Stream<Item = char, Range = &'a str> +
        combine::RangeStreamOnce
    ] {
        use combine::parser::range::recognize;
        use combine::parser::char::*;
        use combine::*;

        macro_rules! white {
            ($prs:expr) => {
                between(
                    skip_many(satisfy(char::is_whitespace)),
                    skip_many(satisfy(char::is_whitespace)),
                    $prs,
                )
            };
        }

        let lambda = char('\\');
        let eq = char('=');
        let flse = white!(string("#f")).map(|_| Ast::Lit(::AstValue::False));
        let ident = || white!(recognize(skip_many1(letter())).map(|chars: &'a str| chars));
        let function = (
            white!(lambda),
            white!(between(char('('), char(')'), many::<Vec<_>, _>(ident()))),
            many::<Vec<_>, _>(expr()),
        ).map(|(_, a, b)| Ast::Lit(::AstValue::Function(a, b)));
        let define = (white!(eq), ident(), expr()).map(|(_, a, b)| Ast::Define(a, Box::new(b)));
        let lit_num = many1::<String, _>(digit())
            .map(|i| Ast::Lit(::AstValue::Int(i.parse().expect("Parsing integer failed"))));
        let call = (expr(), many(expr())).map(|(func, args)| Ast::Call(Box::new(func), args));

        white!(choice!(
            flse,
            lit_num,
            ident().map(Ast::Variable),
            between(char('('), char(')'), choice!(function, define, call))
        ))
    }
}

#[cfg(test)]
mod benches {
    extern crate test;

    use combine::Parser;

    use self::test::{black_box, Bencher};

    use super::{expr, Ast, CompileContext, Value, Variables};

    fn eval(b: &mut Bencher, program: &[Ast], env: Variables) {
        let mut compiled = CompileContext::compile(program, env);
        b.iter(|| black_box(compiled.run()));
    }

    // First we need some helper functions. These are used with the `InbuiltFunc`
    // constructor and act as native functions, similar to how you'd add functions
    // to the global namespace in Lua.
    //
    // This one simply sums the arguments.
    fn add(variables: &[Value]) -> Value {
        let mut out = 0u64;

        for v in variables {
            match v {
                Value::Int(i) => out += i,
                _ => println!("Tried to add a non-int"),
            }
        }

        Value::Int(out)
    }

    // This one checks the arguments for equality. I used `Void` to represent true
    // and `False` to represent false. This is mostly inspired by scheme, where
    // everything is true except for `#f`.
    fn eq(mut variables: &[Value]) -> Value {
        if let Some((last, rest)) = variables.split_last() {
            for v in rest {
                if v != last {
                    return Value::False;
                }
            }

            Value::Void
        } else {
            Value::Void
        }
    }

    // This version of `if` doesn't lazily evaluate its branches, unlike every
    // other programming language in existence. To do lazy evaluation you make
    // the `then` and `else` branches return functions and then call the
    // functions.
    fn if_(variables: &[Value]) -> Value {
        let mut iter = variables.into_iter();
        let (first, second, third) = (
            iter.next().expect("No condition for if"),
            iter.next().expect("No body for if"),
            iter.next().unwrap_or(&Value::Void),
        );
        assert!(iter.next().is_none(), "Too many arguments supplied to `if`");

        match first {
            Value::False => *third,
            _ => *second,
        }
    }

    // Here are our test program strings. Our language looks a lot like Lisp,
    // but it has the important distinction of being totally useless.
    //
    // This string is used to test the performance when programs include
    // deeply-nested structures. Nesting this deep is unlikely but it's a
    // good test for the parser's performance on nesting in general.
    const DEEP_NESTING: &str = "(((((((((((((((((((((((((((((((((((((((((((((test)))))))))))))))))))))))))))))))))))))))))))))";

    // This string is used to test the performance of when programs include
    // many variables of many different names, and many repetitions of the
    // same name. We'd expect real programs to contain lots of variables and
    // so it's important that we get good performance when parsing and
    // evaluating them.
    const MANY_VARIABLES: &str = r"
    ((\(a b c d e f g h i j k l m n o p q r s t u v w x y z)
      (a b c d e f g h i j k l m n o p q r s t u v w x y z)
      (b c d e f g h i j k l m n o p q r s t u v w x y z)
      (c d e f g h i j k l m n o p q r s t u v w x y z)
      (d e f g h i j k l m n o p q r s t u v w x y z)
      (e f g h i j k l m n o p q r s t u v w x y z)
      (f g h i j k l m n o p q r s t u v w x y z)
      (g h i j k l m n o p q r s t u v w x y z)
      (h i j k l m n o p q r s t u v w x y z)
      (i j k l m n o p q r s t u v w x y z)
      (j k l m n o p q r s t u v w x y z)
      (k l m n o p q r s t u v w x y z)
      (l m n o p q r s t u v w x y z)
      (m n o p q r s t u v w x y z)
      (n o p q r s t u v w x y z)
      (o p q r s t u v w x y z)
      (p q r s t u v w x y z)
      (q r s t u v w x y z)
      (r s t u v w x y z)
      (s t u v w x y z)
      (t u v w x y z)
      (u v w x y z)
      (v w x y z)
      (w x y z)
      (x y z)
      (y z)
      (z))
        ignore ignore ignore ignore ignore ignore ignore ignore ignore ignore ignore ignore ignore
        ignore ignore ignore ignore ignore ignore ignore ignore ignore ignore ignore ignore ignore)
        ";

    // This is used to test that function calls aren't unnecessarily
    // expensive. It just passes the same value down and then back up
    // the stack.
    const NESTED_FUNC: &str = r"
    ((\(val)
      ((\(val)
        ((\(val)
          ((\(val)
            ((\(val)
              ((\(val)
                ((\(val)
                  ((\(val)
                    ((\(val)
                      ((\(val)
                        ((\(val)
                          val
                        ) val)
                      ) val)
                    ) val)
                  ) val)
                ) val)
              ) val)
            ) val)
          ) val)
        ) val)
      ) val)
    ) #f)
";

    // This is a more realistic program that uses every feature of
    // the language. It's not useful for finding hotspots but it's
    // definitely useful for seeing improvements.
    const REAL_CODE: &str = r"
(= increment (\(a)
  (add a 1)))
(= someval (increment 2))
(= double (\ (someval)
  (add someval someval)))
(= addfive (\ (first second third fourth fifth) (add first second third fourth fifth)))
(= second (\ (a a) a))
(= rec (\ (a)
  ((if (eq a 10)
       (\() 10)
       (\() (rec (add a 1)))))))
(= ne (\ (a b)
  (not (eq a b))))
(= not (\ (a)
  (if a #f)))

(double 5)
(addfive 1 2 3 4 5)
(second 1 2)
(rec 0)
(ne 1 2)
someval
";

    // Now we run the benchmarks. The parsing ones are very simple...
    #[bench]
    fn parse_deep_nesting(b: &mut Bencher) {
        b.iter(|| black_box(expr().easy_parse(DEEP_NESTING)))
    }

    #[bench]
    fn parse_many_variables(b: &mut Bencher) {
        b.iter(|| black_box(expr().easy_parse(MANY_VARIABLES)))
    }

    #[bench]
    fn parse_nested_func(b: &mut Bencher) {
        b.iter(|| black_box(expr().easy_parse(NESTED_FUNC)))
    }

    #[bench]
    fn parse_real_code(b: &mut Bencher) {
        b.iter(|| black_box(expr().easy_parse(REAL_CODE)))
    }

    // We only test parsing for this one. We could test the speed of
    // evaluating these expressions too but I personally prefer to
    // keep the benchmarks few and representative.
    #[bench]
    fn parse_literals(b: &mut Bencher) {
        let program_text = r"
            ((\()
               0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18 19
              20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39
              40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59
              50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69
              70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89
              90 91 92 93 94 95 96 97 98 99))
        ";

        b.iter(|| black_box(expr().easy_parse(program_text)))
    }

    // For the benchmarks that run the code we have to do a little more
    // work. We need to put some functions in the global namespace that
    // our testing code needs in order to run.
    #[bench]
    fn run_deep_nesting(b: &mut Bencher) {
        // This just returns a function so `((whatever))` (equivalent
        // to `(whatever())()`) does something useful. Specifically
        // it just returns itself. We try to do as little work as
        // possible here so that our benchmark is still testing the
        // interpreter and not this function.
        fn callable(_: &[Value]) -> Value {
            Value::InbuiltFunc(callable)
        }

        let mut env = Variables::new();
        env.set_var("test", Value::InbuiltFunc(callable));

        let (program, _) = expr().easy_parse(DEEP_NESTING).unwrap();

        eval(b, &[program], env);
    }

    #[bench]
    fn run_real_code(b: &mut Bencher) {
        let mut env = Variables::new();

        env.set_var("eq", Value::InbuiltFunc(eq));
        env.set_var("add", Value::InbuiltFunc(add));
        env.set_var("if", Value::InbuiltFunc(if_));

        let (program, _) = ::combine::many1::<Vec<_>, _>(expr())
            .easy_parse(REAL_CODE)
            .unwrap();

        eval(b, &program, env);
    }

    #[bench]
    fn run_many_variables(b: &mut Bencher) {
        // This just takes anything and returns `Void`. We just
        // want a function that can take any number of arguments
        // but we don't want that function to do anything useful
        // since, again, the benchmark should be of the
        // interpreter's code.
        fn ignore(_: &[Value]) -> Value {
            Value::Void
        }

        let (program, _) = expr().easy_parse(MANY_VARIABLES).unwrap();

        let mut env = Variables::new();

        env.set_var("ignore", Value::InbuiltFunc(ignore));

        eval(b, &[program], env);
    }

    #[bench]
    fn run_nested_func(b: &mut Bencher) {
        let (program, _) = expr().easy_parse(NESTED_FUNC).unwrap();
        let mut env = Variables::new();
        eval(b, &[program], env);
    }
}

# ordq
Order keeping job processing queue

Example:

```rust
struct Add;

impl ordq::Work for Add {
    type I = (i32, i32);
    type O = i32;

    fn run(&mut self, x: Self::I) -> Self::O {
        x.0 + x.1
    }
}

# fn test_jobq() {
let (tx, rx) = ordq::new(1024, vec![Add, Add]);

tx.send((1, 2));
tx.send((3, 4));

tx.close();

assert_eq!(rx.recv(), Some(Ok(3)));
assert_eq!(rx.recv(), Some(Ok(7)));
assert_eq!(rx.recv(), None);
assert_eq!(rx.recv(), None);
# }
```

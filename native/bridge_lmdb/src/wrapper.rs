pub struct Wrapper<T> {
    pub value: T,
}

impl<T> Wrapper<T> {
    pub fn new(value: T) -> Wrapper<T> {
        Wrapper { value: value }
    }
}

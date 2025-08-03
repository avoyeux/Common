#[derive(Copy, Clone)]
pub struct IndexGenerator {
    pub nb_of_queues: usize,
    pub current_index: usize,
}

impl IndexGenerator {
    pub fn new(nb_of_queues: usize) -> Self {
        Self {
            nb_of_queues,
            current_index: 0,
        }
    }
}

impl Iterator for IndexGenerator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.current_index;
        self.current_index = (self.current_index + 1) % self.nb_of_queues;
        Some(result)
    }
}

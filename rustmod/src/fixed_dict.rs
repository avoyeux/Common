use pyo3::prelude::*;
use pyo3::exceptions::{PyKeyError, PyValueError};
use std::{slice, usize};

use crate::iterator::IndexGenerator;


#[repr(C)]
#[derive(Copy, Clone)]
struct Slot {
    pub key: usize,
    pub index: IndexGenerator,
    pub counter: usize,
    pub occupied: bool,
}

impl Default for Slot {
    fn default() -> Self {
        Slot {
            key: usize::MAX,
            index: IndexGenerator::new(1),
            counter: usize::MIN,
            occupied: false,
        }
    }
}

struct DictHeader {
    capacity: usize,
    nb_of_queues: usize,
}

#[pyclass]
pub struct FixedDict {
    ptr: *mut u8,
    header: *mut DictHeader,
    slots: *mut Slot,
}
unsafe impl Send for FixedDict {}  // thread safety
unsafe impl Sync for FixedDict {}  // thread safety

#[pymethods]
impl FixedDict {
    /// ! `ptr` buffer >= capacity * size_of::<Slot>() bytes.
    #[new]
    pub unsafe fn new(ptr: usize, capacity: usize, nb_of_queues: usize) -> PyResult<Self> {
        if capacity == 0 {
            return Err(PyValueError::new_err("Capacity must be > 0"));
        }
        if capacity & (capacity - 1) != 0 {
            return Err(PyValueError::new_err("Capacity must be a power of two"));
        }
        if ptr == 0 {
            return Err(PyValueError::new_err("Pointer cannot be null"));
        }

        let raw_ptr = ptr as *mut u8;
        let header = raw_ptr as *mut DictHeader;
        let slots = raw_ptr.add(std::mem::size_of::<DictHeader>()) as *mut Slot;

        // Init in memory
        (*header) = DictHeader {
            capacity,
            nb_of_queues,
        };

        Ok(FixedDict { ptr: raw_ptr, header, slots })
    }

    pub fn set(&self, key: usize, total_tasks: usize) -> PyResult<()> {
        let header = unsafe { &mut *self.header };
        let table = unsafe { slice::from_raw_parts_mut(self.slots, header.capacity) };
        let mut idx = key & (header.capacity - 1);
        let index = IndexGenerator::new(header.nb_of_queues);
        let counter = total_tasks;

        for _ in 0..header.capacity {
            if !table[idx].occupied {
                table[idx] = Slot { key, index, counter, occupied: true };
                return Ok(());
            }
            idx = (idx + 1) & (header.capacity - 1);
        }
        Err(PyKeyError::new_err("No free slot"))
    }

    pub fn get(&mut self, key: usize) -> PyResult<usize> {
        let header = unsafe { &mut *self.header };
        let table = unsafe { slice::from_raw_parts_mut(self.slots, header.capacity) };
        let mut idx = key & (header.capacity - 1);

        for _ in 0..header.capacity {
            let slot = &mut table[idx];
            if slot.occupied && slot.key == key {
                let index = slot.index.next().unwrap();
                slot.counter -= 1;

                if slot.counter == 0 { slot.occupied = false; } // pop key
                return Ok(index);
            }
            idx = (idx + 1) & (header.capacity - 1);
        }
        Err(PyKeyError::new_err("Key not found"))
    }

    pub fn base_address(&self) -> usize {
        self.ptr as usize
    }

    #[staticmethod]
    pub fn total_size(capacity: usize) -> usize {
        std::mem::size_of::<DictHeader>() + capacity * std::mem::size_of::<Slot>()
    }
}

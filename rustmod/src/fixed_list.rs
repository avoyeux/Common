use std::{usize, slice};
use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError};

use crate::iterator::IndexGenerator;


#[repr(C)]
#[derive(Copy, Clone)]
struct Slot {
    pub index: IndexGenerator,
    pub counter: usize,
}
impl Default for Slot {
    fn default() -> Self {
        Slot {
            index: IndexGenerator::new(1),
            counter: usize::MIN,
        }
    }
}

#[repr(C)]
struct ListHeader {
    capacity: usize,
    nb_of_queues: usize,
    end_index: usize,
}

#[pyclass]
pub struct FixedList {
    ptr: *mut u8,
    header: *mut ListHeader,
    slots: *mut Slot,
}
unsafe impl Send for FixedList {}  // thread safety
unsafe impl Sync for FixedList {}  // thread safety

#[pymethods]
impl FixedList {
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
        let header = raw_ptr as *mut ListHeader;
        let slots = raw_ptr.add(std::mem::size_of::<ListHeader>()) as *mut Slot;

        // Init in memory
        (*header) = ListHeader {
            capacity,
            nb_of_queues,
            end_index: 0,
        };
        
        Ok(FixedList { ptr: raw_ptr, header, slots })
    }

    pub fn add(&mut self, total_tasks: usize) -> PyResult<()> {
        let header = unsafe { &mut *self.header };
        let table = unsafe { slice::from_raw_parts_mut(self.slots, header.capacity) };

        if header.end_index >= header.capacity {
            return Err(PyValueError::new_err("FixedList is full"));
        }
        
        let index = IndexGenerator::new(header.nb_of_queues);

        table[header.end_index] = Slot { index: index, counter: total_tasks };
        header.end_index += 1;
        Ok(())
    }

    pub fn next(&mut self) -> PyResult<usize> {
        let header = unsafe { &mut *self.header };

        if header.end_index == 0 {
            return Err(PyValueError::new_err("List is empty"));
        }

        let table = unsafe { slice::from_raw_parts_mut(self.slots, header.capacity) };
        let slot = &mut table[header.end_index - 1];
        let index = slot.index.next().unwrap();

        slot.counter -= 1;
        if slot.counter == 0 { header.end_index -= 1; }
        Ok(index)
    }

    pub fn base_address(&self) -> usize {
        self.ptr as usize
    }

    #[staticmethod]
    pub fn total_size(capacity: usize) -> usize {
        std::mem::size_of::<ListHeader>() + capacity * std::mem::size_of::<Slot>()
    }
}

use pyo3::prelude::*;

mod iterator;
mod fixed_dict;
mod fixed_list;
pub use fixed_dict::FixedDict;
pub use fixed_list::FixedList;

#[pymodule]
fn rustmod(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<FixedDict>()?;
    m.add_class::<FixedList>()?;
    Ok(())
}

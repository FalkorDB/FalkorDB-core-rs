/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::{
    delta_matrix::DeltaMatrix, sparse_matrix_iter::SparseMatrixIter, GraphBLAS::GrB_Index,
};

pub struct DeltaMatrixIter<'a> {
    matrix: Option<&'a DeltaMatrix>,
    m_it: SparseMatrixIter,
    dp_it: SparseMatrixIter,
    min_row: GrB_Index,
    max_row: GrB_Index,
}

impl<'a> DeltaMatrixIter<'a> {
    pub fn new(m: &'a DeltaMatrix) -> DeltaMatrixIter<'a> {
        DeltaMatrixIter {
            matrix: Some(m),
            min_row: 0,
            max_row: u64::MAX,
            m_it: SparseMatrixIter::new(m.m(), 0, u64::MAX),
            dp_it: SparseMatrixIter::new(m.dp(), 0, u64::MAX),
        }
    }

    pub fn attach(
        &mut self,
        m: &'a DeltaMatrix,
    ) {
        self.attach_range(m, 0, u64::MAX);
    }

    pub fn attach_range(
        &mut self,
        m: &'a DeltaMatrix,
        min_row: u64,
        max_row: u64,
    ) {
        self.matrix = Some(m);
        self.min_row = min_row;
        self.max_row = max_row;
        self.m_it = SparseMatrixIter::new(m.m(), min_row, max_row);
        self.dp_it = SparseMatrixIter::new(m.dp(), min_row, max_row);
    }

    pub fn detach(&mut self) {
        self.matrix = None;
    }

    pub fn is_attached(
        &self,
        m: &DeltaMatrix,
    ) -> bool {
        self.matrix.is_some() && std::ptr::eq(self.matrix.unwrap().m(), m.m())
    }

    pub fn iterate_row(
        &mut self,
        row_idx: u64,
    ) {
        self.attach_range(self.matrix.unwrap(), row_idx, row_idx);
    }

    pub fn iterate_range(
        &mut self,
        start_row_idx: u64,
        end_row_idx: u64,
    ) {
        self.attach_range(self.matrix.unwrap(), start_row_idx, end_row_idx);
    }

    pub fn next_bool(&mut self) -> Result<Option<(u64, u64, bool)>, ()> {
        if self.matrix.is_none() {
            return Err(());
        }

        while let Some((i, j, v)) = self.m_it.next_bool(self.max_row) {
            if self
                .matrix
                .unwrap()
                .dm()
                .extract_element_bool(i, j)
                .is_none()
            {
                return Ok(Some((i, j, v)));
            }
        }

        Ok(self.dp_it.next_bool(self.max_row))
    }

    pub fn next_u64(&mut self) -> Result<Option<(u64, u64, u64)>, ()> {
        if self.matrix.is_none() {
            return Err(());
        }

        while let Some((i, j, v)) = self.m_it.next_u64(self.max_row) {
            if self
                .matrix
                .unwrap()
                .dm()
                .extract_element_bool(i, j)
                .is_none()
            {
                return Ok(Some((i, j, v)));
            }
        }

        Ok(self.dp_it.next_u64(self.max_row))
    }

    pub fn reset(&mut self) {
        self.attach_range(self.matrix.unwrap(), self.min_row, self.max_row);
    }
}

#[cfg(test)]
mod tests {

    use crate::graph::matrix::{
        delta_matrix::DeltaMatrix,
        GraphBLAS::{GrB_BOOL, GrB_Mode, GrB_init},
    };

    use super::DeltaMatrixIter;

    #[test]
    fn test_attach() {
        unsafe { GrB_init(GrB_Mode::GrB_NONBLOCKING) };
        let nrows = 100;
        let ncols = 100;
        let a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);
        let mut it = DeltaMatrixIter::new(&a);
        it.attach(&a);

        assert!(it.is_attached(&a));

        it.detach();

        assert!(it.matrix.is_none());
    }

    #[test]
    fn test_next() {
        unsafe { GrB_init(GrB_Mode::GrB_NONBLOCKING) };
        let nrows = 100;
        let ncols = 100;
        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);

        a.set_element_bool(1, 2);
        a.wait(true);
        a.remove_element(1, 2);
        a.set_element_bool(2, 3);

        let mut it = DeltaMatrixIter::new(&a);

        assert!(it.is_attached(&a));
        assert_eq!(it.next_bool(), Ok(Some((2u64, 3u64, true))));
        assert_eq!(it.next_bool(), Ok(None));
        assert_eq!(it.next_bool(), Ok(None));

        it.detach();
        assert_eq!(it.next_bool(), Err(()));
    }
}

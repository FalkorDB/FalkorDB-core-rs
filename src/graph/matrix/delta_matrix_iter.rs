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
    dm_it: SparseMatrixIter,
    min_row: GrB_Index,
    max_row: GrB_Index,
}

impl<'a> DeltaMatrixIter<'a> {
    #[cfg(test)]
    pub fn new(m: &'a DeltaMatrix) -> DeltaMatrixIter<'a> {
        DeltaMatrixIter {
            matrix: Some(m),
            min_row: 0,
            max_row: u64::MAX,
            m_it: SparseMatrixIter::new(m.m(), 0, u64::MAX),
            dp_it: SparseMatrixIter::new(m.dp(), 0, u64::MAX),
            dm_it: SparseMatrixIter::new(m.dm(), 0, u64::MAX),
        }
    }

    /// Initialize the iterator to iterate over [`DeltaMatrix`].
    pub fn attach(
        &mut self,
        m: &'a DeltaMatrix,
    ) {
        self.attach_range(m, 0, u64::MAX);
    }

    /// Initialize the iterator to iterate over [`DeltaMatrix`] within row range.
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
        self.dm_it = SparseMatrixIter::new(m.dm(), min_row, max_row);
    }

    /// Detach the iterator from specific matrix
    pub fn detach(&mut self) {
        self.matrix = None;
    }

    /// Check if the iterator is attached to the given [`DeltaMatrix`].
    pub fn is_attached(
        &self,
        m: &DeltaMatrix,
    ) -> bool {
        self.matrix.is_some() && std::ptr::eq(self.matrix.unwrap().m(), m.m())
    }

    /// Constraint the iterator to iterate over specific row.
    pub fn iterate_row(
        &mut self,
        row_idx: u64,
    ) {
        self.attach_range(self.matrix.unwrap(), row_idx, row_idx);
    }

    /// Constraint the iterator to iterate over specific row range.
    pub fn iterate_range(
        &mut self,
        start_row_idx: u64,
        end_row_idx: u64,
    ) {
        self.attach_range(self.matrix.unwrap(), start_row_idx, end_row_idx);
    }

    /// Returns the next bool of this [`DeltaMatrixIter`].
    ///
    /// # Errors
    ///
    /// This function will return an error if no matrix was attached.
    pub fn next_bool(&mut self) -> Result<Option<(u64, u64)>, ()> {
        if self.matrix.is_none() {
            return Err(());
        }

        while let (Some(i), Some(j)) = (self.m_it.get_row(), self.m_it.get_col()) {
            self.m_it.next(self.max_row);
            if let (Some(mi), Some(mj)) = (self.dm_it.get_row(), self.dm_it.get_col()) {
                if i < mi || (i == mi && j < mj) {
                    return Ok(Some((i, j)));
                }
                debug_assert!(i == mi && j == mj);
                self.dm_it.next(self.max_row);
            } else {
                return Ok(Some((i, j)));
            }
        }

        if let (Some(i), Some(j)) = (self.dp_it.get_row(), self.dp_it.get_col()) {
            self.dp_it.next(self.max_row);
            return Ok(Some((i, j)));
        }

        Ok(None)
    }

    /// Returns the next u64 of this [`DeltaMatrixIter`].
    ///
    /// # Errors
    ///
    /// This function will return an error if no matrix was attached.
    pub fn next_u64(&mut self) -> Result<Option<(u64, u64, u64)>, ()> {
        if self.matrix.is_none() {
            return Err(());
        }

        while let (Some(i), Some(j), Some(v)) = (
            self.m_it.get_row(),
            self.m_it.get_col(),
            self.m_it.get_u64(),
        ) {
            self.m_it.next(self.max_row);
            if let (Some(mi), Some(mj)) = (self.dm_it.get_row(), self.dm_it.get_col()) {
                if i < mi || (i == mi && j < mj) {
                    return Ok(Some((i, j, v)));
                }
                debug_assert!(i == mi && j == mj);
                self.dm_it.next(self.max_row);
            } else {
                return Ok(Some((i, j, v)));
            }
        }

        if let (Some(i), Some(j), Some(v)) = (
            self.dp_it.get_row(),
            self.dp_it.get_col(),
            self.dp_it.get_u64(),
        ) {
            self.dp_it.next(self.max_row);
            return Ok(Some((i, j, v)));
        }

        Ok(None)
    }

    /// Reset this [`DeltaMatrixIter`] to start from the beggining.
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

    fn test_init() {
        unsafe { GrB_init(GrB_Mode::GrB_NONBLOCKING) };
    }

    #[test]
    fn test_attach() {
        test_init();
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
        test_init();
        let nrows = 100;
        let ncols = 100;
        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);

        a.set_element_bool(1, 2);
        a.wait(true);
        a.remove_element(1, 2);
        a.set_element_bool(2, 3);

        let mut it = DeltaMatrixIter::new(&a);

        assert!(it.is_attached(&a));
        assert_eq!(it.next_bool(), Ok(Some((2u64, 3u64))));
        assert_eq!(it.next_bool(), Ok(None));
        assert_eq!(it.next_bool(), Ok(None));

        it.detach();
        assert_eq!(it.next_bool(), Err(()));
    }
}

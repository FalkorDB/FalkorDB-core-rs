/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::{mem::MaybeUninit, ptr::null_mut};

use crate::binding::{
    cmutex::CMutex,
    graph::{ConfigOptionField, Config_Option_get},
};

use super::{
    sparse_matrix::SparseMatrix,
    GraphBLAS::{
        GrB_ALL, GrB_BOOL, GrB_DESC_RSC, GrB_DESC_RSCT0, GrB_DESC_RT0, GrB_DESC_S, GrB_Scalar_free,
        GrB_Scalar_new, GrB_Semiring, GrB_Type, GxB_ANY_PAIR_BOOL, GxB_HYPERSPARSE, GxB_SPARSE,
    },
};

/// Delta Matrix solve the issue of writing to a sparse matrix with high number of nnz
/// By using additional matrices with limited number of nnz
/// m represent the stable matrix
/// delta_pluse recent n additions
/// delta_minus recent m deletions
pub struct DeltaMatrix {
    dirty: bool,
    matrix: SparseMatrix,
    delta_plus: SparseMatrix,
    delta_minus: SparseMatrix,
    transposed: Option<Box<DeltaMatrix>>,
    mutex: Option<CMutex>,
}

impl DeltaMatrix {
    /// Creates a new [`DeltaMatrix`].
    pub fn new(
        ty: GrB_Type,
        nrows: u64,
        ncols: u64,
        transpose: bool,
    ) -> Self {
        unsafe {
            let mut x = Self {
                dirty: false,
                matrix: SparseMatrix::new(ty, nrows, ncols),
                delta_plus: SparseMatrix::new(ty, nrows, ncols),
                delta_minus: SparseMatrix::new(GrB_BOOL, nrows, ncols),
                transposed: if transpose {
                    let mut t = Box::new(Self {
                        dirty: false,
                        matrix: SparseMatrix::new(GrB_BOOL, ncols, nrows),
                        delta_plus: SparseMatrix::new(GrB_BOOL, ncols, nrows),
                        delta_minus: SparseMatrix::new(GrB_BOOL, ncols, nrows),
                        transposed: None,
                        mutex: None,
                    });
                    t.matrix.set_sparsity(GxB_SPARSE | GxB_HYPERSPARSE);
                    t.delta_plus.set_sparsity(GxB_HYPERSPARSE);
                    t.delta_plus.set_always_hyper();
                    t.delta_minus.set_sparsity(GxB_HYPERSPARSE);
                    t.delta_minus.set_always_hyper();
                    Some(t)
                } else {
                    None
                },
                mutex: Some(CMutex::new()),
            };
            x.matrix.set_sparsity(GxB_SPARSE | GxB_HYPERSPARSE);
            x.delta_plus.set_sparsity(GxB_HYPERSPARSE);
            x.delta_plus.set_always_hyper();
            x.delta_minus.set_sparsity(GxB_HYPERSPARSE);
            x.delta_minus.set_always_hyper();
            x
        }
    }

    /// Returns the transposed of this [`DeltaMatrix`].
    pub fn transposed(&self) -> Option<&Box<DeltaMatrix>> {
        self.transposed.as_ref()
    }

    /// Returns the transposed of this [`DeltaMatrix`].
    pub fn transposed_mut(&mut self) -> Option<&mut Box<DeltaMatrix>> {
        self.transposed.as_mut()
    }

    fn set_dirty(
        &mut self,
        dirty: bool,
    ) {
        self.dirty = dirty;
        if let Some(t) = self.transposed.as_mut() {
            t.set_dirty(dirty);
        }
    }

    /// Returns a reference to the m of this [`DeltaMatrix`].
    pub fn m(&self) -> &SparseMatrix {
        &self.matrix
    }

    pub fn m_mut(&mut self) -> &mut SparseMatrix {
        &mut self.matrix
    }

    /// Returns a reference to the delta plus of this [`DeltaMatrix`].
    pub fn dp(&self) -> &SparseMatrix {
        &self.delta_plus
    }

    /// Returns a reference to the delta minus of this [`DeltaMatrix`].
    pub fn dm(&self) -> &SparseMatrix {
        &self.delta_minus
    }

    /// Returns the number of rows of this [`DeltaMatrix`].
    pub fn nrows(&self) -> u64 {
        self.matrix.nrows()
    }

    /// Returns the number of columns of this [`DeltaMatrix`].
    pub fn ncols(&self) -> u64 {
        self.matrix.ncols()
    }

    /// Returns the number of non zero values of this [`DeltaMatrix`].
    pub fn nvals(&self) -> u64 {
        self.matrix.nvals() + self.delta_plus.nvals() - self.delta_minus.nvals()
    }

    /// Resize the [`DeltaMatrix`].
    pub fn resize(
        &mut self,
        nrows_new: u64,
        ncols_new: u64,
    ) {
        if let Some(t) = self.transposed.as_mut() {
            t.resize(ncols_new, nrows_new);
        }
        self.matrix.resize(nrows_new, ncols_new);
        self.delta_plus.resize(nrows_new, ncols_new);
        self.delta_minus.resize(nrows_new, ncols_new);
    }

    /// Remove element from this [`DeltaMatrix`].
    pub fn remove_element(
        &mut self,
        i: u64,
        j: u64,
    ) {
        if let Some(t) = self.transposed.as_mut() {
            t.remove_element(j, i);
        }

        // if the value presented in m set dm otherwise remove from dp
        if self.matrix.extract_element_bool(i, j).is_some() {
            self.delta_minus.set_element_bool(true, i, j);
        } else {
            self.delta_plus.remove_element(i, j);
        }
        self.set_dirty(true);
    }

    /// Sets the element of this [`DeltaMatrix`].
    pub fn set_element_bool(
        &mut self,
        i: u64,
        j: u64,
    ) {
        if let Some(t) = self.transposed.as_mut() {
            t.set_element_bool(j, i);
        }

        // if the value marked as deleted in dm remove it
        // otherwise if it is not presented in m set it in dp
        if self.delta_minus.extract_element_bool(i, j).is_some() {
            self.delta_minus.remove_element(i, j);
            self.set_dirty(true);
        } else if self.matrix.extract_element_bool(i, j).is_none() {
            self.delta_plus.set_element_bool(true, i, j);
            self.set_dirty(true);
        }
    }

    /// Sets the element of this [`DeltaMatrix`].
    pub fn set_element_u64(
        &mut self,
        x: u64,
        i: u64,
        j: u64,
    ) {
        if let Some(t) = self.transposed.as_mut() {
            t.set_element_bool(j, i);
        }

        // if the value marked as deleted in dm remove it
        // otherwise if it is not presented in m set it in dp
        if self.delta_minus.extract_element_bool(i, j).is_some() {
            self.delta_minus.remove_element(i, j);
            self.matrix.set_element_u64(x, i, j);
            self.set_dirty(true);
        } else if self.matrix.extract_element_u64(i, j).is_none() {
            self.delta_plus.set_element_u64(x, i, j);
            self.set_dirty(true);
        } else {
            self.matrix.set_element_u64(x, i, j);
        }
    }

    /// Get the value at position of this [`DeltaMatrix`].
    pub fn extract_element_bool(
        &self,
        i: u64,
        j: u64,
    ) -> Option<bool> {
        // if the value presented in dp return true
        // if it is deleted in dm return no value
        // otherwise return it from m
        if self.delta_plus.extract_element_bool(i, j).is_some() {
            Some(true)
        } else if self.delta_minus.extract_element_bool(i, j).is_some() {
            None
        } else {
            self.matrix.extract_element_bool(i, j)
        }
    }

    /// Get the value at position of this [`DeltaMatrix`].
    pub fn extract_element_u64(
        &self,
        i: u64,
        j: u64,
    ) -> Option<u64> {
        // if the value presented in dp return true
        // if it is deleted in dm return no value
        // otherwise return it from m
        if let Some(v) = self.delta_plus.extract_element_u64(i, j) {
            Some(v)
        } else if self.delta_minus.extract_element_bool(i, j).is_some() {
            None
        } else {
            self.matrix.extract_element_u64(i, j)
        }
    }

    /// Remove all presented elements from this [`DeltaMatrix`].
    pub fn remove_elements(
        &mut self,
        mask: &SparseMatrix,
    ) {
        debug_assert!(self.transposed.is_none());

        unsafe {
            let mut s = MaybeUninit::uninit();
            GrB_Scalar_new(s.as_mut_ptr(), GrB_BOOL);
            // delete all presented elements in dp
            self.delta_plus.assign_scalar(
                mask,
                s.assume_init(),
                GrB_ALL,
                self.nrows(),
                GrB_ALL,
                self.ncols(),
                GrB_DESC_S,
            );
            // delete elements presented in m  by marking them as deleted in dm
            self.delta_minus.assign(
                mask,
                &self.matrix,
                GrB_ALL,
                self.nrows(),
                GrB_ALL,
                self.ncols(),
                GrB_DESC_S,
            );
            self.set_dirty(true);
            GrB_Scalar_free(s.as_mut_ptr());
        }
    }

    /// Clear this [`DeltaMatrix`].
    pub fn clear(&mut self) {
        debug_assert!(self.transposed.is_none());

        self.matrix.clear();
        self.delta_plus.clear();
        self.delta_minus.clear();
        self.set_dirty(true);
    }

    /// Copy this [`DeltaMatrix`].
    pub fn copy(
        &mut self,
        a: &DeltaMatrix,
    ) {
        debug_assert!(self.transposed.is_none());

        self.matrix.copy(&a.matrix);
        self.delta_plus.copy(&a.delta_plus);
        self.delta_minus.copy(&a.delta_minus);
    }

    /// Multiply m by n and the result is in this [`DeltaMatrix`].
    pub fn mxm(
        &mut self,
        semiring: GrB_Semiring,
        m: &DeltaMatrix,
        n: &DeltaMatrix,
    ) {
        unsafe {
            let (mask, desc) = if n.delta_minus.nvals() > 0 {
                let mut mask = SparseMatrix::new(GrB_BOOL, self.nrows(), self.ncols());
                mask.mxm(
                    None,
                    &m.matrix,
                    &n.delta_minus,
                    GxB_ANY_PAIR_BOOL,
                    null_mut(),
                );
                if mask.nvals() > 0 {
                    (Some(mask), GrB_DESC_RSC)
                } else {
                    (None, null_mut())
                }
            } else {
                (None, null_mut())
            };

            let accum = if n.delta_plus.nvals() > 0 {
                let mut accum = SparseMatrix::new(GrB_BOOL, self.nrows(), self.ncols());
                accum.mxm(None, &m.matrix, &n.delta_plus, semiring, null_mut());
                if accum.nvals() > 0 {
                    Some(accum)
                } else {
                    None
                }
            } else {
                None
            };

            self.matrix
                .mxm(mask.as_ref(), &m.matrix, &n.matrix, semiring, desc);
            if let Some(accum) = accum {
                self.matrix
                    .element_wise_add(None, None, Some(&accum), GxB_ANY_PAIR_BOOL);
            }
        }
    }

    /// Elementwise add m and n the result is in this [`DeltaMatrix`].
    pub fn element_wise_add(
        &mut self,
        semiring: GrB_Semiring,
        m: &DeltaMatrix,
        n: &DeltaMatrix,
    ) {
        match (
            m.delta_minus.nvals() > 0 || m.delta_plus.nvals() > 0,
            n.delta_minus.nvals() > 0 || n.delta_plus.nvals() > 0,
        ) {
            (true, true) => {
                self.matrix
                    .element_wise_add(None, Some(&m.export()), Some(&n.export()), semiring);
            }
            (true, false) => {
                self.matrix
                    .element_wise_add(None, Some(&m.export()), Some(&n.matrix), semiring);
            }
            (false, true) => {
                self.matrix
                    .element_wise_add(None, Some(&m.matrix), Some(&n.export()), semiring);
            }
            (false, false) => {
                self.matrix
                    .element_wise_add(None, Some(&m.matrix), Some(&n.matrix), semiring);
            }
        }
    }

    /// Returns [`SparseMatrix`] by computing m-dm+dp of this [`DeltaMatrix`].
    pub fn export(&self) -> SparseMatrix {
        let mut m = SparseMatrix::new(unsafe { GrB_BOOL }, self.nrows(), self.ncols());
        if self.delta_minus.nvals() > 0 {
            m.transpose(
                Some(&self.delta_minus),
                null_mut(),
                Some(&self.matrix),
                unsafe { GrB_DESC_RSCT0 },
            );
        } else {
            m.transpose(None, null_mut(), Some(&self.matrix), unsafe {
                GrB_DESC_RT0
            });
        }
        if self.delta_plus.nvals() > 0 {
            m.element_wise_add(None, None, Some(&self.delta_plus), unsafe {
                GxB_ANY_PAIR_BOOL
            });
        }

        m
    }

    /// Returns if there are pending changes in this [`DeltaMatrix`].
    pub fn pending(&self) -> bool {
        if self
            .transposed
            .as_ref()
            .map(|t| t.pending())
            .unwrap_or_default()
        {
            return true;
        }

        self.matrix.pending() || self.delta_plus.pending() || self.delta_minus.pending()
    }

    /// Apply pending changes on this [`DeltaMatrix`].
    /// if force_sync is true apply dp and dm on m
    /// otherwise just apply pending on the m, dp, dm
    pub fn wait(
        &mut self,
        force_sync: bool,
    ) {
        if let Some(t) = self.transposed.as_mut() {
            t.wait(force_sync);
        }

        let mut delta_max_pending_changes = MaybeUninit::uninit();
        unsafe {
            Config_Option_get(
                ConfigOptionField::DELTA_MAX_PENDING_CHANGES,
                delta_max_pending_changes.as_mut_ptr(),
            )
        };
        let delta_max_pending_changes = unsafe { delta_max_pending_changes.assume_init() };

        self.sync(force_sync, delta_max_pending_changes);
        self.set_dirty(false);
    }

    fn sync(
        &mut self,
        force_sync: bool,
        delta_max_pending_changes: u64,
    ) {
        if force_sync {
            self.sync_deletions();
            self.sync_additions();
        } else {
            if self.delta_minus.nvals() > delta_max_pending_changes {
                self.sync_deletions();
            }

            if self.delta_plus.nvals() > delta_max_pending_changes {
                self.sync_additions();
            }
        }

        self.matrix.wait();
        self.delta_plus.wait();
        self.delta_minus.wait();
    }

    fn sync_deletions(&mut self) {
        self.matrix
            .transpose(Some(&self.delta_minus), null_mut(), None, unsafe {
                GrB_DESC_RSCT0
            });
        self.delta_minus.clear();
    }

    fn sync_additions(&mut self) {
        unsafe {
            self.matrix.assign(
                &self.delta_plus,
                &self.delta_plus,
                GrB_ALL,
                self.nrows(),
                GrB_ALL,
                self.ncols(),
                GrB_DESC_S,
            );
        }
        self.delta_plus.clear();
    }

    /// Check if need to resize or to apply pending changes on this [`DeltaMatrix`].
    pub fn synchronize(
        &mut self,
        nrows: u64,
        ncols: u64,
    ) {
        if !(self.nrows() < nrows || self.ncols() < ncols || self.dirty) {
            return;
        }

        self.mutex.as_mut().unwrap().lock();

        if self.nrows() < nrows || self.ncols() < ncols {
            self.resize(nrows, ncols);
        }

        if self.dirty {
            self.wait(false);
        }

        self.mutex.as_mut().unwrap().unlock();
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::null_mut;

    use libc::{c_char, rand};

    use crate::{
        binding::graph::{ConfigOptionField, Config_Option_set},
        graph::matrix::{
            sparse_matrix::SparseMatrix,
            GraphBLAS::{
                GrB_BOOL, GrB_LAND, GrB_Matrix_eWiseMult_BinaryOp, GrB_Mode, GrB_init,
                GxB_ANY_PAIR_BOOL, GxB_Format_Value, GxB_Global_Option_set, GxB_Option_Field,
            },
        },
    };

    use super::DeltaMatrix;

    fn test_init() {
        unsafe {
            GrB_init(GrB_Mode::GrB_NONBLOCKING);
            GxB_Global_Option_set(GxB_Option_Field::GxB_FORMAT, GxB_Format_Value::GxB_BY_ROW);
            Config_Option_set(
                ConfigOptionField::DELTA_MAX_PENDING_CHANGES,
                "10000\0".as_ptr() as *const c_char,
                null_mut(),
            );
        };
    }

    #[test]
    fn test_new_matrix() {
        test_init();
        let nrows = 100;
        let ncols = 100;
        let a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);
        assert_eq!(a.m().nvals(), 0);
        assert_eq!(a.delta_plus.nvals(), 0);
        assert_eq!(a.delta_minus.nvals(), 0);
        assert_eq!(a.nrows(), nrows);
        assert_eq!(a.ncols(), ncols);
        assert_eq!(a.nvals(), 0);
        assert!(!a.dirty);
        assert!(a.transposed().is_none());

        let a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, true);
        assert_eq!(a.m().nvals(), 0);
        assert_eq!(a.delta_plus.nvals(), 0);
        assert_eq!(a.delta_minus.nvals(), 0);
        assert_eq!(a.nrows(), nrows);
        assert_eq!(a.ncols(), ncols);
        assert_eq!(a.nvals(), 0);
        assert!(!a.dirty);
        assert!(a.transposed().is_some());
        assert_eq!(a.transposed().unwrap().m().nvals(), 0);
        assert_eq!(a.transposed().unwrap().delta_plus.nvals(), 0);
        assert_eq!(a.transposed().unwrap().delta_minus.nvals(), 0);
        assert_eq!(a.transposed().unwrap().nrows(), ncols);
        assert_eq!(a.transposed().unwrap().ncols(), nrows);
        assert_eq!(a.transposed().unwrap().nvals(), 0);
        assert!(!a.transposed().unwrap().dirty);
        assert!(a.transposed().unwrap().transposed().is_none());
    }

    #[test]
    fn test_simple_set() {
        test_init();
        let nrows = 100;
        let ncols = 100;
        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);

        let i = 0;
        let j = 1;
        a.set_element_bool(i, j);

        assert_eq!(a.extract_element_bool(i, j), Some(true));
        assert_eq!(a.nvals(), 1);
        assert!(a.dirty);
        assert_eq!(a.m().nvals(), 0);
        assert_eq!(a.delta_minus.nvals(), 0);
        assert_eq!(a.delta_plus.nvals(), 1);

        a.wait(false);

        a.set_element_bool(i, j);

        assert_eq!(a.m().nvals(), 0);
        assert_eq!(a.delta_minus.nvals(), 0);
        assert_eq!(a.delta_plus.nvals(), 1);
    }

    #[test]
    fn test_set() {
        test_init();
        let nrows = 100;
        let ncols = 100;
        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);

        let i = 0;
        let j = 1;

        a.set_element_bool(i, j);

        a.wait(true);

        a.remove_element(i, j);

        a.set_element_bool(i, j);

        assert_eq!(a.nvals(), 1);
        assert_eq!(a.delta_minus.nvals(), 0);
        assert_eq!(a.delta_plus.nvals(), 0);
    }

    #[test]
    fn test_del() {
        test_init();
        let nrows = 100;
        let ncols = 100;
        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);

        let i = 0;
        let j = 1;

        a.remove_element(i, j);

        assert_eq!(a.delta_minus.nvals(), 0);
        assert_eq!(a.delta_plus.nvals(), 0);

        a.set_element_bool(i, j);
        a.remove_element(i, j);

        assert!(a.dirty);
        assert_eq!(a.nvals(), 0);
        assert_eq!(a.matrix.nvals(), 0);
        assert_eq!(a.delta_minus.nvals(), 0);
        assert_eq!(a.delta_plus.nvals(), 0);

        a.set_element_bool(i, j);
        a.wait(true);
        a.remove_element(i, j);

        assert_eq!(a.nvals(), 0);
        assert_eq!(a.matrix.nvals(), 1);
        assert_eq!(a.delta_minus.nvals(), 1);
        assert_eq!(a.delta_plus.nvals(), 0);

        a.wait(true);

        assert_eq!(a.nvals(), 0);
        assert_eq!(a.matrix.nvals(), 0);
        assert_eq!(a.delta_minus.nvals(), 0);
        assert_eq!(a.delta_plus.nvals(), 0);

        a.set_element_bool(i, j);
        a.wait(true);
        a.remove_element(i, j);
        a.set_element_bool(i, j);

        assert_eq!(a.nvals(), 1);
        assert_eq!(a.matrix.nvals(), 1);
        assert_eq!(a.delta_minus.nvals(), 0);
        assert_eq!(a.delta_plus.nvals(), 0);
    }

    #[test]
    fn test_transpose() {
        test_init();
        let nrows = 100;
        let ncols = 100;
        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, true);

        let i = 0;
        let j = 1;

        a.set_element_bool(i, j);

        let t = a.transposed.as_ref().unwrap();

        assert_eq!(t.extract_element_bool(j, i), Some(true));
        assert_eq!(t.nvals(), 1);
        assert!(t.dirty);
        assert_eq!(t.m().nvals(), 0);
        assert_eq!(t.delta_minus.nvals(), 0);
        assert_eq!(t.delta_plus.nvals(), 1);

        a.wait(true);

        let t = a.transposed.as_ref().unwrap();

        assert_eq!(t.m().nvals(), 1);
        assert_eq!(t.delta_minus.nvals(), 0);
        assert_eq!(t.delta_plus.nvals(), 0);

        a.remove_element(i, j);

        let t = a.transposed.as_ref().unwrap();

        assert!(t.dirty);
        assert_eq!(t.m().nvals(), 1);
        assert_eq!(t.delta_minus.nvals(), 1);
        assert_eq!(t.delta_plus.nvals(), 0);

        a.wait(true);

        let t = a.transposed.as_ref().unwrap();

        assert_eq!(t.m().nvals(), 0);
        assert_eq!(t.delta_minus.nvals(), 0);
        assert_eq!(t.delta_plus.nvals(), 0);
    }

    fn matrix_eq(
        a: &SparseMatrix,
        b: &SparseMatrix,
    ) {
        assert_eq!(a.nrows(), b.nrows());
        assert_eq!(a.ncols(), b.ncols());
        assert_eq!(a.nvals(), b.nvals());

        let c = SparseMatrix::new(unsafe { GrB_BOOL }, a.nrows(), a.ncols());
        unsafe {
            GrB_Matrix_eWiseMult_BinaryOp(
                c.grb_matrix_ref(),
                null_mut(),
                null_mut(),
                GrB_LAND,
                a.grb_matrix_ref(),
                b.grb_matrix_ref(),
                null_mut(),
            );
        }
        assert_eq!(c.nvals(), a.nvals());
    }

    #[test]
    fn test_fuzzy() {
        test_init();
        let nrows = 100;
        let ncols = 100;
        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, true);
        let mut m = SparseMatrix::new(unsafe { GrB_BOOL }, nrows, ncols);
        let mut t = SparseMatrix::new(unsafe { GrB_BOOL }, ncols, nrows);

        let mut additions_i = Vec::with_capacity(10000);
        let mut additions_j = Vec::with_capacity(10000);
        for i in 0..10000 {
            if i < 10 || unsafe { rand() } % 100 > 20 {
                let i = unsafe { rand() as u64 } % nrows;
                let j = unsafe { rand() as u64 } % ncols;
                a.set_element_bool(i, j);
                m.set_element_bool(true, i, j);
                t.set_element_bool(true, j, i);
                additions_i.push(i);
                additions_j.push(j);
            } else {
                let r = (unsafe { rand() } % additions_i.len() as i32) as usize;
                let i = additions_i[r];
                let j = additions_j[r];
                a.remove_element(i, j);
                m.remove_element(i, j);
                t.remove_element(j, i);
            }
        }

        a.wait(true);

        matrix_eq(&a.matrix, &m);
        matrix_eq(&a.transposed.unwrap().matrix, &t);
    }

    #[test]
    fn test_export_no_changes() {
        test_init();
        let nrows = 100;
        let ncols = 100;
        let i = 0;
        let j = 1;

        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);

        let n = a.export();
        matrix_eq(&a.matrix, &n);

        a.set_element_bool(i, j);
        a.wait(true);
        let n = a.export();
        matrix_eq(&a.matrix, &n);
    }

    #[test]
    fn test_export_pending_changes() {
        test_init();
        let nrows = 100;
        let ncols = 100;

        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);

        a.set_element_bool(0, 0);
        a.set_element_bool(1, 1);
        a.wait(true);
        a.remove_element(0, 0);
        a.set_element_bool(2, 2);
        let n = a.export();
        a.wait(true);

        matrix_eq(&a.matrix, &n);
    }

    #[test]
    fn test_copy() {
        test_init();
        let nrows = 100;
        let ncols = 100;

        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);
        let mut b = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);

        a.set_element_bool(0, 0);
        a.set_element_bool(1, 1);
        a.wait(true);
        a.remove_element(0, 0);
        a.set_element_bool(2, 2);

        b.copy(&a);

        matrix_eq(&a.matrix, &b.matrix);
        matrix_eq(&a.delta_minus, &b.delta_minus);
        matrix_eq(&a.delta_minus, &b.delta_minus);
    }

    #[test]
    fn test_mxm() {
        test_init();
        let nrows = 100;
        let ncols = 100;

        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);
        let mut b = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);
        let mut c = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);
        let mut d = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);

        a.set_element_bool(0, 1);
        a.set_element_bool(2, 3);
        b.set_element_bool(1, 2);
        b.set_element_bool(3, 4);

        a.wait(true);
        b.wait(true);

        b.remove_element(1, 2);
        b.set_element_bool(1, 3);

        c.mxm(unsafe { GxB_ANY_PAIR_BOOL }, &a, &b);

        b.wait(true);

        d.mxm(unsafe { GxB_ANY_PAIR_BOOL }, &a, &b);

        matrix_eq(&c.matrix, &d.matrix);
    }

    #[test]
    fn test_resize() {
        test_init();
        let nrows = 100;
        let ncols = 200;

        let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, true);
        let t = a.transposed.as_ref().unwrap();

        assert_eq!(a.nrows(), nrows);
        assert_eq!(a.matrix.nrows(), nrows);
        assert_eq!(a.delta_plus.nrows(), nrows);
        assert_eq!(a.delta_minus.nrows(), nrows);

        assert_eq!(t.nrows(), ncols);
        assert_eq!(t.matrix.nrows(), ncols);
        assert_eq!(t.delta_plus.nrows(), ncols);
        assert_eq!(t.delta_minus.nrows(), ncols);

        assert_eq!(a.ncols(), ncols);
        assert_eq!(a.matrix.ncols(), ncols);
        assert_eq!(a.delta_plus.ncols(), ncols);
        assert_eq!(a.delta_minus.ncols(), ncols);

        assert_eq!(t.ncols(), nrows);
        assert_eq!(t.matrix.ncols(), nrows);
        assert_eq!(t.delta_plus.ncols(), nrows);
        assert_eq!(t.delta_minus.ncols(), nrows);

        let nrows = nrows * 2;
        let ncols = ncols * 2;

        a.resize(nrows, ncols);

        let t = a.transposed.as_ref().unwrap();

        assert_eq!(a.nrows(), nrows);
        assert_eq!(a.matrix.nrows(), nrows);
        assert_eq!(a.delta_plus.nrows(), nrows);
        assert_eq!(a.delta_minus.nrows(), nrows);

        assert_eq!(t.nrows(), ncols);
        assert_eq!(t.matrix.nrows(), ncols);
        assert_eq!(t.delta_plus.nrows(), ncols);
        assert_eq!(t.delta_minus.nrows(), ncols);

        assert_eq!(a.ncols(), ncols);
        assert_eq!(a.matrix.ncols(), ncols);
        assert_eq!(a.delta_plus.ncols(), ncols);
        assert_eq!(a.delta_minus.ncols(), ncols);

        assert_eq!(t.ncols(), nrows);
        assert_eq!(t.matrix.ncols(), nrows);
        assert_eq!(t.delta_plus.ncols(), nrows);
        assert_eq!(t.delta_minus.ncols(), nrows);

        let nrows = nrows / 2;
        let ncols = ncols / 2;

        a.resize(nrows, ncols);

        let t = a.transposed.as_ref().unwrap();

        assert_eq!(a.nrows(), nrows);
        assert_eq!(a.matrix.nrows(), nrows);
        assert_eq!(a.delta_plus.nrows(), nrows);
        assert_eq!(a.delta_minus.nrows(), nrows);

        assert_eq!(t.nrows(), ncols);
        assert_eq!(t.matrix.nrows(), ncols);
        assert_eq!(t.delta_plus.nrows(), ncols);
        assert_eq!(t.delta_minus.nrows(), ncols);

        assert_eq!(a.ncols(), ncols);
        assert_eq!(a.matrix.ncols(), ncols);
        assert_eq!(a.delta_plus.ncols(), ncols);
        assert_eq!(a.delta_minus.ncols(), ncols);

        assert_eq!(t.ncols(), nrows);
        assert_eq!(t.matrix.ncols(), nrows);
        assert_eq!(t.delta_plus.ncols(), nrows);
        assert_eq!(t.delta_minus.ncols(), nrows);
    }
}

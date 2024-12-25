/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

 use crate::binding::graph::{ConfigOptionField, Config_Option_get};
 use parking_lot::Mutex;
 use std::sync::Arc;
 use std::{mem::MaybeUninit, ptr::null_mut};
 
 use super::{
     sparse_matrix::SparseMatrix,
     GraphBLAS::{
         GrB_ALL, GrB_BOOL, GrB_DESC_RSC, GrB_DESC_RSCT0, GrB_DESC_RT0, GrB_DESC_S, GrB_DESC_T1,
         GrB_Descriptor, GrB_Scalar_free, GrB_Scalar_new, GrB_Semiring, GrB_Type, GxB_ANY_PAIR_BOOL,
         GxB_HYPERSPARSE, GxB_SPARSE,
     },
 };
 
 struct DeltaMatrixBase {
     pub matrix: SparseMatrix,
     pub delta_plus: SparseMatrix,
     pub delta_minus: SparseMatrix,
 }
 
 /// Delta Matrix solve the issue of writing to a sparse matrix with high number of nnz
 /// By using additional matrices with limited number of nnz
 /// m represent the stable matrix
 /// delta_pluse recent n additions
 /// delta_minus recent m deletions
 pub struct DeltaMatrix {
     dirty: bool,
     matrix: DeltaMatrixBase,
     transpose: Option<DeltaMatrixBase>,
     mutex: Arc<Mutex<()>>,
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
                 matrix: DeltaMatrixBase {
                     matrix: SparseMatrix::new(ty, nrows, ncols),
                     delta_plus: SparseMatrix::new(ty, nrows, ncols),
                     delta_minus: SparseMatrix::new(GrB_BOOL, nrows, ncols),
                 },
                 transpose: if transpose {
                     Some(DeltaMatrixBase {
                         matrix: SparseMatrix::new(GrB_BOOL, ncols, nrows),
                         delta_plus: SparseMatrix::new(GrB_BOOL, ncols, nrows),
                         delta_minus: SparseMatrix::new(GrB_BOOL, ncols, nrows),
                     })
                 } else {
                     None
                 },
                 mutex: Arc::new(Mutex::new(())),
             };
             x.matrix.matrix.set_sparsity(GxB_SPARSE | GxB_HYPERSPARSE);
             x.matrix.delta_plus.set_sparsity(GxB_HYPERSPARSE);
             x.matrix.delta_plus.set_always_hyper();
             x.matrix.delta_minus.set_sparsity(GxB_HYPERSPARSE);
             x.matrix.delta_minus.set_always_hyper();
             if let Some(x) = x.transpose.as_mut() {
                 x.matrix.set_sparsity(GxB_SPARSE | GxB_HYPERSPARSE);
                 x.delta_plus.set_sparsity(GxB_HYPERSPARSE);
                 x.delta_plus.set_always_hyper();
                 x.delta_minus.set_sparsity(GxB_HYPERSPARSE);
                 x.delta_minus.set_always_hyper();
             }
             x
         }
     }
 
     /// Returns a reference to the m of this [`DeltaMatrix`].
     pub fn m(
         &self,
         transpose: bool,
     ) -> &SparseMatrix {
         if transpose {
             &self.transpose.as_ref().unwrap().matrix
         } else {
             &self.matrix.matrix
         }
     }
 
     pub fn m_mut(
         &mut self,
         transpose: bool,
     ) -> &mut SparseMatrix {
         if transpose {
             &mut self.transpose.as_mut().unwrap().matrix
         } else {
             &mut self.matrix.matrix
         }
     }
 
     /// Returns a reference to the delta plus of this [`DeltaMatrix`].
     pub fn dp(
         &self,
         transpose: bool,
     ) -> &SparseMatrix {
         if transpose {
             &self.transpose.as_ref().unwrap().delta_plus
         } else {
             &self.matrix.delta_plus
         }
     }
 
     /// Returns a reference to the delta minus of this [`DeltaMatrix`].
     pub fn dm(
         &self,
         transpose: bool,
     ) -> &SparseMatrix {
         if transpose {
             &self.transpose.as_ref().unwrap().delta_minus
         } else {
             &self.matrix.delta_minus
         }
     }
 
     /// Returns the number of rows of this [`DeltaMatrix`].
     pub fn nrows(&self) -> u64 {
         self.matrix.matrix.nrows()
     }
 
     /// Returns the number of columns of this [`DeltaMatrix`].
     pub fn ncols(&self) -> u64 {
         self.matrix.matrix.ncols()
     }
 
     /// Returns the number of non zero values of this [`DeltaMatrix`].
     pub fn nvals(&self) -> u64 {
         self.matrix.matrix.nvals() + self.matrix.delta_plus.nvals()
             - self.matrix.delta_minus.nvals()
     }
 
     /// Resize the [`DeltaMatrix`].
     pub fn resize(
         &mut self,
         nrows_new: u64,
         ncols_new: u64,
     ) {
         if let Some(t) = self.transpose.as_mut() {
             t.matrix.resize(ncols_new, nrows_new);
             t.delta_plus.resize(ncols_new, nrows_new);
             t.delta_minus.resize(ncols_new, nrows_new);
         }
 
         self.matrix.matrix.resize(nrows_new, ncols_new);
         self.matrix.delta_plus.resize(nrows_new, ncols_new);
         self.matrix.delta_minus.resize(nrows_new, ncols_new);
     }
 
     /// Remove element from this [`DeltaMatrix`].
     pub fn remove_element(
         &mut self,
         i: u64,
         j: u64,
     ) {
         // if the value presented in m set dm otherwise remove from dp
         if self.matrix.matrix.extract_element_bool(i, j).is_some() {
             self.matrix.delta_minus.set_element_bool(true, i, j);
             if let Some(t) = self.transpose.as_mut() {
                 t.delta_minus.set_element_bool(true, j, i);
             }
         } else {
             self.matrix.delta_plus.remove_element(i, j);
             if let Some(t) = self.transpose.as_mut() {
                 t.delta_plus.remove_element(j, i);
             }
         }
         self.dirty = true;
     }
 
     /// Sets the element of this [`DeltaMatrix`].
     pub fn set_element_bool(
         &mut self,
         i: u64,
         j: u64,
     ) {
         // if the value marked as deleted in dm remove it
         // otherwise if it is not presented in m set it in dp
         if self.matrix.delta_minus.extract_element_bool(i, j).is_some() {
             self.matrix.delta_minus.remove_element(i, j);
             if let Some(t) = self.transpose.as_mut() {
                 t.delta_minus.remove_element(j, i);
             }
             self.dirty = true;
         } else if self.matrix.matrix.extract_element_bool(i, j).is_none() {
             self.matrix.delta_plus.set_element_bool(true, i, j);
             if let Some(t) = self.transpose.as_mut() {
                 t.delta_plus.set_element_bool(true, j, i);
             }
             self.dirty = true;
         }
     }
 
     /// Sets the element of this [`DeltaMatrix`].
     pub fn set_element_u64(
         &mut self,
         x: u64,
         i: u64,
         j: u64,
     ) {
         // if the value marked as deleted in dm remove it
         // otherwise if it is not presented in m set it in dp
         if self.matrix.delta_minus.extract_element_bool(i, j).is_some() {
             self.matrix.delta_minus.remove_element(i, j);
             self.matrix.matrix.set_element_u64(x, i, j);
             if let Some(t) = self.transpose.as_mut() {
                 t.delta_minus.remove_element(j, i);
                 t.matrix.set_element_bool(true, j, i);
             }
             self.dirty = true;
         } else if self.matrix.matrix.extract_element_u64(i, j).is_none() {
             self.matrix.delta_plus.set_element_u64(x, i, j);
             if let Some(t) = self.transpose.as_mut() {
                 t.delta_plus.set_element_bool(true, j, i);
             }
             self.dirty = true;
         } else {
             self.matrix.matrix.set_element_u64(x, i, j);
             if let Some(t) = self.transpose.as_mut() {
                 t.matrix.set_element_bool(true, j, i);
             }
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
         if self.matrix.delta_plus.extract_element_bool(i, j).is_some() {
             Some(true)
         } else if self.matrix.delta_minus.extract_element_bool(i, j).is_some() {
             None
         } else {
             self.matrix.matrix.extract_element_bool(i, j)
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
         if let Some(v) = self.matrix.delta_plus.extract_element_u64(i, j) {
             Some(v)
         } else if self.matrix.delta_minus.extract_element_bool(i, j).is_some() {
             None
         } else {
             self.matrix.matrix.extract_element_u64(i, j)
         }
     }
 
     /// Remove all presented elements from this [`DeltaMatrix`].
     pub fn remove_elements(
         &mut self,
         mask: &SparseMatrix,
     ) {
         debug_assert!(self.transpose.is_none());
 
         unsafe {
             let mut s = MaybeUninit::uninit();
             GrB_Scalar_new(s.as_mut_ptr(), GrB_BOOL);
             // delete all presented elements in dp
             self.matrix.delta_plus.assign_scalar(
                 mask,
                 s.assume_init(),
                 GrB_ALL,
                 self.nrows(),
                 GrB_ALL,
                 self.ncols(),
                 GrB_DESC_S,
             );
             // delete elements presented in m  by marking them as deleted in dm
             self.matrix.delta_minus.assign(
                 mask,
                 &self.matrix.matrix,
                 GrB_ALL,
                 self.nrows(),
                 GrB_ALL,
                 self.ncols(),
                 GrB_DESC_S,
             );
             self.dirty = true;
             GrB_Scalar_free(s.as_mut_ptr());
         }
     }
 
     /// Clear this [`DeltaMatrix`].
     pub fn clear(&mut self) {
         debug_assert!(self.transpose.is_none());
 
         self.matrix.matrix.clear();
         self.matrix.delta_plus.clear();
         self.matrix.delta_minus.clear();
         self.dirty = false;
     }
 
     /// Copy this [`DeltaMatrix`].
     pub fn copy(
         &mut self,
         a: &DeltaMatrix,
     ) {
         debug_assert!(self.transpose.is_none());
 
         self.matrix.matrix.copy(&a.matrix.matrix);
         self.matrix.delta_plus.copy(&a.matrix.delta_plus);
         self.matrix.delta_minus.copy(&a.matrix.delta_minus);
     }
 
     /// Multiply m by n and the result is in this [`DeltaMatrix`].
     pub fn mxm(
         &mut self,
         semiring: GrB_Semiring,
         m: &DeltaMatrix,
         n: &DeltaMatrix,
         desc: GrB_Descriptor,
     ) {
         unsafe {
             let n = if desc == GrB_DESC_T1 && n.transpose.is_some() {
                 n.transpose.as_ref().unwrap()
             } else {
                 &n.matrix
             };
             let (mask, desc) = if n.delta_minus.nvals() > 0 {
                 let mut mask = SparseMatrix::new(GrB_BOOL, self.nrows(), self.ncols());
                 mask.mxm(
                     None,
                     &m.matrix.matrix,
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
                 accum.mxm(None, &m.matrix.matrix, &n.delta_plus, semiring, null_mut());
                 if accum.nvals() > 0 {
                     Some(accum)
                 } else {
                     None
                 }
             } else {
                 None
             };
 
             self.matrix
                 .matrix
                 .mxm(mask.as_ref(), &m.matrix.matrix, &n.matrix, semiring, desc);
             if let Some(accum) = accum {
                 self.matrix
                     .matrix
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
         desc: GrB_Descriptor,
     ) {
         match (
             m.matrix.delta_minus.nvals() > 0 || m.matrix.delta_plus.nvals() > 0,
             n.matrix.delta_minus.nvals() > 0 || n.matrix.delta_plus.nvals() > 0,
         ) {
             (true, true) => {
                 self.matrix.matrix.element_wise_add(
                     None,
                     Some(&m.export(false)),
                     Some(&n.export(desc == unsafe { GrB_DESC_T1 })),
                     semiring,
                 );
             }
             (true, false) => {
                 self.matrix.matrix.element_wise_add(
                     None,
                     Some(&m.export(false)),
                     Some(n.m(desc == unsafe { GrB_DESC_T1 })),
                     semiring,
                 );
             }
             (false, true) => {
                 self.matrix.matrix.element_wise_add(
                     None,
                     Some(&m.matrix.matrix),
                     Some(&n.export(desc == unsafe { GrB_DESC_T1 })),
                     semiring,
                 );
             }
             (false, false) => {
                 self.matrix.matrix.element_wise_add(
                     None,
                     Some(&m.matrix.matrix),
                     Some(n.m(desc == unsafe { GrB_DESC_T1 })),
                     semiring,
                 );
             }
         }
     }
 
     /// Returns [`SparseMatrix`] by computing m-dm+dp of this [`DeltaMatrix`].
     pub fn export(
         &self,
         transpose: bool,
     ) -> SparseMatrix {
         let s = if transpose {
             self.transpose.as_ref().unwrap()
         } else {
             &self.matrix
         };
         let mut m = SparseMatrix::new(unsafe { GrB_BOOL }, self.nrows(), self.ncols());
         if s.delta_minus.nvals() > 0 {
             m.transpose(Some(&s.delta_minus), null_mut(), Some(&s.matrix), unsafe {
                 GrB_DESC_RSCT0
             });
         } else {
             m.transpose(None, null_mut(), Some(&s.matrix), unsafe { GrB_DESC_RT0 });
         }
         if s.delta_plus.nvals() > 0 {
             m.element_wise_add(None, None, Some(&s.delta_plus), unsafe {
                 GxB_ANY_PAIR_BOOL
             });
         }
 
         m
     }
 
     /// Returns if there are pending changes in this [`DeltaMatrix`].
     pub fn pending(&self) -> bool {
         self.matrix.matrix.pending()
             || self.matrix.delta_plus.pending()
             || self.matrix.delta_minus.pending()
     }
 
     /// Apply pending changes on this [`DeltaMatrix`].
     /// if force_sync is true apply dp and dm on m
     /// otherwise just apply pending on the m, dp, dm
     pub fn wait(
         &mut self,
         force_sync: bool,
     ) {
         let mut delta_max_pending_changes = MaybeUninit::uninit();
         unsafe {
             Config_Option_get(
                 ConfigOptionField::DELTA_MAX_PENDING_CHANGES,
                 delta_max_pending_changes.as_mut_ptr(),
             )
         };
         let delta_max_pending_changes = unsafe { delta_max_pending_changes.assume_init() };
 
         self.sync(force_sync, delta_max_pending_changes);
         self.dirty = false;
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
             if self.matrix.delta_minus.nvals() > delta_max_pending_changes {
                 self.sync_deletions();
             }
 
             if self.matrix.delta_plus.nvals() > delta_max_pending_changes {
                 self.sync_additions();
             }
         }
 
         self.matrix.matrix.wait();
         self.matrix.delta_plus.wait();
         self.matrix.delta_minus.wait();
         if let Some(t) = self.transpose.as_mut() {
             t.matrix.wait();
             t.delta_plus.wait();
             t.delta_minus.wait();
         }
     }
 
     fn sync_deletions(&mut self) {
         self.matrix
             .matrix
             .transpose(Some(&self.matrix.delta_minus), null_mut(), None, unsafe {
                 GrB_DESC_RSCT0
             });
         self.matrix.delta_minus.clear();
         if let Some(t) = self.transpose.as_mut() {
             t.matrix
                 .transpose(Some(&t.delta_minus), null_mut(), None, unsafe {
                     GrB_DESC_RSCT0
                 });
             t.delta_minus.clear();
         }
     }
 
     fn sync_additions(&mut self) {
         let nrows = self.nrows();
         let ncols = self.ncols();
         unsafe {
             self.matrix.matrix.assign(
                 &self.matrix.delta_plus,
                 &self.matrix.delta_plus,
                 GrB_ALL,
                 nrows,
                 GrB_ALL,
                 ncols,
                 GrB_DESC_S,
             );
         }
         self.matrix.delta_plus.clear();
 
         if let Some(t) = self.transpose.as_mut() {
             unsafe {
                 t.matrix.assign(
                     &t.delta_plus,
                     &t.delta_plus,
                     GrB_ALL,
                     ncols,
                     GrB_ALL,
                     nrows,
                     GrB_DESC_S,
                 );
             }
             t.delta_plus.clear();
         }
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
         let mutex = Arc::clone(&self.mutex);
         let _guard = mutex.lock();
 
         if self.nrows() < nrows || self.ncols() < ncols {
             self.resize(nrows, ncols);
         }
 
         if self.dirty {
             self.wait(false);
         }
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
         assert_eq!(a.matrix.matrix.nvals(), 0);
         assert_eq!(a.matrix.delta_plus.nvals(), 0);
         assert_eq!(a.matrix.delta_minus.nvals(), 0);
         assert_eq!(a.nrows(), nrows);
         assert_eq!(a.ncols(), ncols);
         assert_eq!(a.nvals(), 0);
         assert!(!a.dirty);
         assert!(a.transpose.is_none());
 
         let a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, true);
         assert_eq!(a.matrix.matrix.nvals(), 0);
         assert_eq!(a.matrix.delta_plus.nvals(), 0);
         assert_eq!(a.matrix.delta_minus.nvals(), 0);
         assert_eq!(a.nrows(), nrows);
         assert_eq!(a.ncols(), ncols);
         assert_eq!(a.nvals(), 0);
         assert!(!a.dirty);
         assert!(a.transpose.is_some());
         assert_eq!(a.transpose.as_ref().unwrap().matrix.nvals(), 0);
         assert_eq!(a.transpose.as_ref().unwrap().delta_plus.nvals(), 0);
         assert_eq!(a.transpose.as_ref().unwrap().delta_minus.nvals(), 0);
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
         assert_eq!(a.matrix.matrix.nvals(), 0);
         assert_eq!(a.matrix.delta_minus.nvals(), 0);
         assert_eq!(a.matrix.delta_plus.nvals(), 1);
 
         a.wait(false);
 
         a.set_element_bool(i, j);
 
         assert_eq!(a.matrix.matrix.nvals(), 0);
         assert_eq!(a.matrix.delta_minus.nvals(), 0);
         assert_eq!(a.matrix.delta_plus.nvals(), 1);
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
         assert_eq!(a.matrix.delta_minus.nvals(), 0);
         assert_eq!(a.matrix.delta_plus.nvals(), 0);
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
 
         assert_eq!(a.matrix.delta_minus.nvals(), 0);
         assert_eq!(a.matrix.delta_plus.nvals(), 0);
 
         a.set_element_bool(i, j);
         a.remove_element(i, j);
 
         assert!(a.dirty);
         assert_eq!(a.nvals(), 0);
         assert_eq!(a.matrix.matrix.nvals(), 0);
         assert_eq!(a.matrix.delta_minus.nvals(), 0);
         assert_eq!(a.matrix.delta_plus.nvals(), 0);
 
         a.set_element_bool(i, j);
         a.wait(true);
         a.remove_element(i, j);
 
         assert_eq!(a.nvals(), 0);
         assert_eq!(a.matrix.matrix.nvals(), 1);
         assert_eq!(a.matrix.delta_minus.nvals(), 1);
         assert_eq!(a.matrix.delta_plus.nvals(), 0);
 
         a.wait(true);
 
         assert_eq!(a.nvals(), 0);
         assert_eq!(a.matrix.matrix.nvals(), 0);
         assert_eq!(a.matrix.delta_minus.nvals(), 0);
         assert_eq!(a.matrix.delta_plus.nvals(), 0);
 
         a.set_element_bool(i, j);
         a.wait(true);
         a.remove_element(i, j);
         a.set_element_bool(i, j);
 
         assert_eq!(a.nvals(), 1);
         assert_eq!(a.matrix.matrix.nvals(), 1);
         assert_eq!(a.matrix.delta_minus.nvals(), 0);
         assert_eq!(a.matrix.delta_plus.nvals(), 0);
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
 
         let t = a.transpose.as_ref().unwrap();
 
         assert_eq!(t.matrix.nvals(), 0);
         assert_eq!(t.delta_minus.nvals(), 0);
         assert_eq!(t.delta_plus.nvals(), 1);
 
         a.wait(true);
 
         let t = a.transpose.as_ref().unwrap();
 
         assert_eq!(t.matrix.nvals(), 1);
         assert_eq!(t.delta_minus.nvals(), 0);
         assert_eq!(t.delta_plus.nvals(), 0);
 
         a.remove_element(i, j);
 
         let t = a.transpose.as_ref().unwrap();
 
         assert_eq!(t.matrix.nvals(), 1);
         assert_eq!(t.delta_minus.nvals(), 1);
         assert_eq!(t.delta_plus.nvals(), 0);
 
         a.wait(true);
 
         let t = a.transpose.as_ref().unwrap();
 
         assert_eq!(t.matrix.nvals(), 0);
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
 
         matrix_eq(&a.matrix.matrix, &m);
         matrix_eq(&a.transpose.unwrap().matrix, &t);
     }
 
     #[test]
     fn test_export_no_changes() {
         test_init();
         let nrows = 100;
         let ncols = 100;
         let i = 0;
         let j = 1;
 
         let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, false);
 
         let n = a.export(false);
         matrix_eq(&a.matrix.matrix, &n);
 
         a.set_element_bool(i, j);
         a.wait(true);
         let n = a.export(false);
         matrix_eq(&a.matrix.matrix, &n);
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
         let n = a.export(false);
         a.wait(true);
 
         matrix_eq(&a.matrix.matrix, &n);
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
 
         matrix_eq(&a.matrix.matrix, &b.matrix.matrix);
         matrix_eq(&a.matrix.delta_minus, &b.matrix.delta_minus);
         matrix_eq(&a.matrix.delta_minus, &b.matrix.delta_minus);
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
 
         c.mxm(unsafe { GxB_ANY_PAIR_BOOL }, &a, &b, null_mut());
 
         b.wait(true);
 
         d.mxm(unsafe { GxB_ANY_PAIR_BOOL }, &a, &b, null_mut());
 
         matrix_eq(&c.matrix.matrix, &d.matrix.matrix);
     }
 
     #[test]
     fn test_resize() {
         test_init();
         let nrows = 100;
         let ncols = 200;
 
         let mut a = DeltaMatrix::new(unsafe { GrB_BOOL }, nrows, ncols, true);
         let t = a.transpose.as_ref().unwrap();
 
         assert_eq!(a.nrows(), nrows);
         assert_eq!(a.matrix.matrix.nrows(), nrows);
         assert_eq!(a.matrix.delta_plus.nrows(), nrows);
         assert_eq!(a.matrix.delta_minus.nrows(), nrows);
 
         assert_eq!(t.matrix.nrows(), ncols);
         assert_eq!(t.delta_plus.nrows(), ncols);
         assert_eq!(t.delta_minus.nrows(), ncols);
 
         assert_eq!(a.ncols(), ncols);
         assert_eq!(a.matrix.matrix.ncols(), ncols);
         assert_eq!(a.matrix.delta_plus.ncols(), ncols);
         assert_eq!(a.matrix.delta_minus.ncols(), ncols);
 
         assert_eq!(t.matrix.ncols(), nrows);
         assert_eq!(t.delta_plus.ncols(), nrows);
         assert_eq!(t.delta_minus.ncols(), nrows);
 
         let nrows = nrows * 2;
         let ncols = ncols * 2;
 
         a.resize(nrows, ncols);
 
         let t = a.transpose.as_ref().unwrap();
 
         assert_eq!(a.nrows(), nrows);
         assert_eq!(a.matrix.matrix.nrows(), nrows);
         assert_eq!(a.matrix.delta_plus.nrows(), nrows);
         assert_eq!(a.matrix.delta_minus.nrows(), nrows);
 
         assert_eq!(t.matrix.nrows(), ncols);
         assert_eq!(t.delta_plus.nrows(), ncols);
         assert_eq!(t.delta_minus.nrows(), ncols);
 
         assert_eq!(a.ncols(), ncols);
         assert_eq!(a.matrix.matrix.ncols(), ncols);
         assert_eq!(a.matrix.delta_plus.ncols(), ncols);
         assert_eq!(a.matrix.delta_minus.ncols(), ncols);
 
         assert_eq!(t.matrix.ncols(), nrows);
         assert_eq!(t.delta_plus.ncols(), nrows);
         assert_eq!(t.delta_minus.ncols(), nrows);
 
         let nrows = nrows / 2;
         let ncols = ncols / 2;
 
         a.resize(nrows, ncols);
 
         let t = a.transpose.as_ref().unwrap();
 
         assert_eq!(a.nrows(), nrows);
         assert_eq!(a.matrix.matrix.nrows(), nrows);
         assert_eq!(a.matrix.delta_plus.nrows(), nrows);
         assert_eq!(a.matrix.delta_minus.nrows(), nrows);
 
         assert_eq!(t.matrix.nrows(), ncols);
         assert_eq!(t.delta_plus.nrows(), ncols);
         assert_eq!(t.delta_minus.nrows(), ncols);
 
         assert_eq!(a.ncols(), ncols);
         assert_eq!(a.matrix.matrix.ncols(), ncols);
         assert_eq!(a.matrix.delta_plus.ncols(), ncols);
         assert_eq!(a.matrix.delta_minus.ncols(), ncols);
 
         assert_eq!(t.matrix.ncols(), nrows);
         assert_eq!(t.delta_plus.ncols(), nrows);
         assert_eq!(t.delta_minus.ncols(), nrows);
     }
 }
 
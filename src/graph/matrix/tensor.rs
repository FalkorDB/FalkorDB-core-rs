/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::{
    mem::MaybeUninit,
    ptr::{self, null_mut},
};

use crate::binding::graph::{Edge, EdgeID, NodeID};

use super::{
    delta_matrix::DeltaMatrix,
    delta_matrix_iter::DeltaMatrixIter,
    GraphBLAS::{
        GB_Iterator_opaque, GrB_BOOL, GrB_INDEX_MAX, GrB_Info, GrB_UINT64, GrB_Vector,
        GrB_Vector_free, GrB_Vector_new, GrB_Vector_nvals, GrB_Vector_removeElement,
        GrB_Vector_setElement_BOOL, GrB_Vector_wait, GrB_WaitMode, GxB_Vector_Iterator_attach,
        GxB_Vector_Iterator_getIndex, GxB_Vector_Iterator_next, GxB_Vector_Iterator_seek,
    },
};

pub fn single_edge(current_edge: EdgeID) -> bool {
    current_edge as u64 & (1u64 << (u64::BITS as usize - 1)) == 0
}

pub fn set_msb(meid: EdgeID) -> u64 {
    meid as u64 | (1u64 << (u64::BITS as usize - 1))
}

pub fn clear_msb(meid: EdgeID) -> u64 {
    meid as u64 & !(1u64 << (u64::BITS as usize - 1))
}

pub struct Tensor {
    pub m: DeltaMatrix,
}

impl Tensor {
    pub fn new(
        nrows: u64,
        ncols: u64,
    ) -> Self {
        Self {
            m: DeltaMatrix::new(unsafe { GrB_UINT64 }, nrows, ncols, true),
        }
    }

    pub fn set_element(
        &mut self,
        src: u64,
        dest: u64,
        id: u64,
    ) {
        if let Some(current_edge) = self.m.extract_element_u64(src, dest) {
            unsafe {
                if single_edge(current_edge) {
                    let mut v = MaybeUninit::uninit();
                    GrB_Vector_new(v.as_mut_ptr(), GrB_BOOL, GrB_INDEX_MAX);
                    let v = v.assume_init();
                    self.m.set_element_u64(set_msb(v as u64), src, dest);
                    GrB_Vector_setElement_BOOL(v, true, current_edge);
                    GrB_Vector_setElement_BOOL(v, true, id);
                    GrB_Vector_wait(v, GrB_WaitMode::GrB_MATERIALIZE);
                } else {
                    let v = clear_msb(current_edge) as GrB_Vector;
                    GrB_Vector_setElement_BOOL(v, true, id);
                    GrB_Vector_wait(v, GrB_WaitMode::GrB_MATERIALIZE);
                }
            }
        } else {
            self.m.set_element_u64(id, src, dest);
        }
    }

    pub fn set_elements(
        &mut self,
        edges: &mut [*mut Edge],
    ) {
        let mut delayed = Vec::new();

        let mut i = 0;
        while i < edges.len() {
            let e = unsafe { edges[i].as_ref().unwrap() };
            let src = e.src_id;
            let dest = e.dest_id;

            unsafe {
                let mut j = i + 1;
                while j < edges.len()
                    && edges[j].as_ref().unwrap().src_id == src
                    && edges[j].as_ref().unwrap().dest_id == dest
                {
                    j += 1;
                }

                match self.m.extract_element_u64(src, dest) {
                    Some(id) => {
                        if single_edge(id) {
                            let mut v = MaybeUninit::uninit();
                            GrB_Vector_new(v.as_mut_ptr(), GrB_BOOL, GrB_INDEX_MAX);
                            let v = v.assume_init();
                            self.m.set_element_u64(set_msb(v as u64), src, dest);
                            GrB_Vector_setElement_BOOL(v, true, id);
                            for k in i..j {
                                GrB_Vector_setElement_BOOL(v, true, edges[k].as_ref().unwrap().id);
                            }
                            GrB_Vector_wait(v, GrB_WaitMode::GrB_MATERIALIZE);
                        } else {
                            let v = clear_msb(id) as GrB_Vector;
                            for k in i..j {
                                GrB_Vector_setElement_BOOL(v, true, edges[k].as_ref().unwrap().id);
                            }
                            GrB_Vector_wait(v, GrB_WaitMode::GrB_MATERIALIZE);
                        }
                    }
                    None => {
                        delayed.push(i);
                        delayed.push(j);
                    }
                }
                i = j;
            }
        }

        for i in (0..delayed.len()).step_by(2) {
            let from = delayed[i];
            let to = delayed[i + 1];
            unsafe {
                let src = edges[from].as_ref().unwrap().src_id;
                let dest = edges[from].as_ref().unwrap().dest_id;

                if to - from == 1 {
                    self.m
                        .set_element_u64(edges[from].as_ref().unwrap().id, src, dest);
                } else {
                    let mut v = MaybeUninit::uninit();
                    GrB_Vector_new(v.as_mut_ptr(), GrB_BOOL, GrB_INDEX_MAX);
                    let v = v.assume_init();
                    self.m.set_element_u64(set_msb(v as u64), src, dest);
                    for j in from..to {
                        GrB_Vector_setElement_BOOL(v, true, edges[j].as_ref().unwrap().id);
                    }
                    GrB_Vector_wait(v, GrB_WaitMode::GrB_MATERIALIZE);
                }
            }
        }
    }

    pub fn remove_elements_flat(
        &mut self,
        edges: &[Edge],
    ) {
        for edge in edges {
            let src = edge.src_id;
            let dest = edge.dest_id;
            self.m.remove_element(src, dest);
        }
    }

    pub fn remove_elements(
        &mut self,
        edges: &[Edge],
    ) -> Vec<usize> {
        let mut cleared_entries = Vec::new();

        let mut i = 0;
        while i < edges.len() {
            let e = edges[i];
            let src = e.src_id;
            let dest = e.dest_id;

            let mut j = i + 1;
            while j < edges.len() && edges[j].src_id == src && edges[j].dest_id == dest {
                j += 1;
            }

            let d = j - i;
            let id = self.m.extract_element_u64(src, dest).unwrap();
            if single_edge(id) {
                debug_assert_eq!(d, 1);
                cleared_entries.push(i);
            } else {
                let mut v = clear_msb(id) as GrB_Vector;
                let mut nvals = MaybeUninit::uninit();
                unsafe {
                    GrB_Vector_nvals(nvals.as_mut_ptr(), v);
                    if d == nvals.assume_init() as usize {
                        GrB_Vector_free(&mut v);
                        cleared_entries.push(i);
                    } else if d + 1 == nvals.assume_init() as usize {
                        for k in i..j {
                            GrB_Vector_removeElement(v, edges[k].id);
                        }
                        let mut it = MaybeUninit::uninit();
                        GxB_Vector_Iterator_attach(it.as_mut_ptr(), v, null_mut());
                        let mut it = it.assume_init();
                        GxB_Vector_Iterator_seek(&mut it, 0);
                        let index = GxB_Vector_Iterator_getIndex(&mut it);
                        self.m.set_element_u64(index, src, dest);
                        GrB_Vector_free(&mut v);
                    } else {
                        for k in i..j {
                            GrB_Vector_removeElement(v, edges[k].id);
                        }
                        GrB_Vector_wait(v, GrB_WaitMode::GrB_MATERIALIZE);
                    }
                }
            }

            i = j;
        }

        for i in cleared_entries.iter() {
            let src = edges[*i].src_id;
            let dest = edges[*i].dest_id;
            self.m.remove_element(src, dest);
        }

        cleared_entries
    }

    pub fn iter(
        &self,
        src_id: u64,
        dest_id: u64,
    ) -> TensorIterator {
        TensorIterator::new(self, src_id, dest_id)
    }

    pub fn iter_range(
        &self,
        min_src_id: u64,
        max_src_id: u64,
        transposed: bool,
    ) -> TensorRangeIterator {
        TensorRangeIterator::new(self, min_src_id, max_src_id, transposed)
    }

    pub fn row_degree(
        &self,
        id: u64,
    ) -> u64 {
        let mut degree = 0;
        let mut it = DeltaMatrixIter::new_range(&self.m, id, id);
        while let Ok(Some((_, _, id))) = it.next_u64() {
            if single_edge(id) {
                degree += 1;
            } else {
                let v = clear_msb(id) as GrB_Vector;
                let mut nvals = MaybeUninit::uninit();
                unsafe {
                    GrB_Vector_nvals(nvals.as_mut_ptr(), v);
                    degree += nvals.assume_init();
                }
            }
        }
        degree
    }

    pub fn col_degree(
        &self,
        id: u64,
    ) -> u64 {
        let mut degree = 0;
        let mut it = DeltaMatrixIter::new_range(self.m.transposed().unwrap(), id, id);
        while let Ok(Some((dest, src))) = it.next_bool() {
            let id = self.m.extract_element_u64(src, dest).unwrap();
            if single_edge(id) {
                degree += 1;
            } else {
                let v = clear_msb(id) as GrB_Vector;
                let mut nvals = MaybeUninit::uninit();
                unsafe {
                    GrB_Vector_nvals(nvals.as_mut_ptr(), v);
                    degree += nvals.assume_init();
                }
            }
        }
        degree
    }
}

pub struct TensorIterator {
    eit: Option<GB_Iterator_opaque>,
    src_id: u64,
    dest_id: u64,
    edge_id: u64,
    deleted: bool,
}
impl TensorIterator {
    fn new(
        m: &Tensor,
        src_id: u64,
        dest_id: u64,
    ) -> Self {
        let id = m.m.extract_element_u64(src_id, dest_id);
        Self {
            eit: None,
            src_id: src_id,
            dest_id: dest_id,
            edge_id: if id.is_some() { id.unwrap() } else { u64::MAX },
            deleted: id.is_none(),
        }
    }
}

pub struct TensorRangeIterator<'a> {
    m: &'a Tensor,
    rit: Option<DeltaMatrixIter<'a>>,
    eit: Option<GB_Iterator_opaque>,
    src_id: u64,
    dest_id: u64,
    edge_id: u64,
    transposed: bool,
    attached: bool,
}

impl<'a> TensorRangeIterator<'a> {
    fn new(
        m: &'a Tensor,
        min_src_id: u64,
        max_src_id: u64,
        transposed: bool,
    ) -> Self {
        let r = if transposed {
            m.m.transposed().unwrap()
        } else {
            &m.m
        };
        Self {
            m,
            rit: Some(DeltaMatrixIter::new_range(r, min_src_id, max_src_id)),
            eit: None,
            src_id: min_src_id,
            dest_id: u64::MAX,
            edge_id: u64::MAX,
            transposed: transposed,
            attached: true,
        }
    }

    pub fn is_attached(
        &self,
        other: &Tensor,
    ) -> bool {
        if !self.attached {
            return false;
        }
        ptr::eq(self.m.m.m().grb_matrix_ref(), other.m.m().grb_matrix_ref())
    }
}

impl Iterator for TensorIterator {
    type Item = (NodeID, NodeID, EdgeID);

    fn next(&mut self) -> Option<Self::Item> {
        if self.deleted {
            return None;
        }
        if single_edge(self.edge_id) {
            self.deleted = true;
            Some((self.src_id, self.dest_id, self.edge_id))
        } else {
            if self.eit.is_none() {
                let v = clear_msb(self.edge_id) as GrB_Vector;
                let mut it = MaybeUninit::uninit();
                unsafe {
                    GxB_Vector_Iterator_attach(it.as_mut_ptr(), v, null_mut());
                    GxB_Vector_Iterator_seek(it.as_mut_ptr(), 0)
                };
                self.eit = Some(unsafe { it.assume_init() });
            }
            unsafe {
                let edge_id = GxB_Vector_Iterator_getIndex(self.eit.as_mut().unwrap());
                let info = GxB_Vector_Iterator_next(self.eit.as_mut().unwrap());
                if info == GrB_Info::GxB_EXHAUSTED {
                    self.eit = None;
                    self.deleted = true;
                }
                return Some((self.src_id, self.dest_id, edge_id));
            }
        }
    }
}

impl Iterator for TensorRangeIterator<'_> {
    type Item = (NodeID, NodeID, EdgeID);

    fn next(&mut self) -> Option<Self::Item> {
        if self.eit.is_some() {
            unsafe {
                self.edge_id = GxB_Vector_Iterator_getIndex(self.eit.as_mut().unwrap());
                let info = GxB_Vector_Iterator_next(self.eit.as_mut().unwrap());
                if info == GrB_Info::GxB_EXHAUSTED {
                    self.eit = None;
                }
                return Some((self.src_id, self.dest_id, self.edge_id));
            }
        }

        if self.transposed {
            if let Ok(Some((dest_id, src_id))) = self.rit.as_mut().unwrap().next_bool() {
                self.src_id = src_id;
                self.dest_id = dest_id;
                self.edge_id = self.m.m.extract_element_u64(src_id, dest_id).unwrap();
                if !single_edge(self.edge_id) {
                    let v = clear_msb(self.edge_id) as GrB_Vector;
                    let mut it = MaybeUninit::uninit();
                    unsafe {
                        GxB_Vector_Iterator_attach(it.as_mut_ptr(), v, null_mut());
                        GxB_Vector_Iterator_seek(it.as_mut_ptr(), 0)
                    };
                    self.eit = Some(unsafe { it.assume_init() });

                    unsafe {
                        self.edge_id = GxB_Vector_Iterator_getIndex(self.eit.as_mut().unwrap());
                        let info = GxB_Vector_Iterator_next(self.eit.as_mut().unwrap());
                        debug_assert!(info == GrB_Info::GrB_SUCCESS);
                    }
                }
                return Some((src_id, dest_id, self.edge_id));
            }
        } else {
            if let Ok(Some((src_id, dest_id, current_edge))) = self.rit.as_mut().unwrap().next_u64()
            {
                self.src_id = src_id;
                self.dest_id = dest_id;
                self.edge_id = current_edge;
                if !single_edge(current_edge) {
                    let v = clear_msb(current_edge) as GrB_Vector;
                    unsafe {
                        let mut it = MaybeUninit::uninit();
                        GxB_Vector_Iterator_attach(it.as_mut_ptr(), v, null_mut());
                        GxB_Vector_Iterator_seek(it.as_mut_ptr(), 0);
                        self.eit = Some(it.assume_init());

                        self.edge_id = GxB_Vector_Iterator_getIndex(self.eit.as_mut().unwrap());
                        let info = GxB_Vector_Iterator_next(self.eit.as_mut().unwrap());
                        debug_assert!(info == GrB_Info::GrB_SUCCESS);
                    }
                }
                return Some((src_id, dest_id, self.edge_id));
            }
        }

        None
    }
}

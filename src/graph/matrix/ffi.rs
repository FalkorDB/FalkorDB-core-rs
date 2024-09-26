/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::ptr::null_mut;

use super::{
    delta_matrix::DeltaMatrix,
    delta_matrix_iter::DeltaMatrixIter,
    GraphBLAS::{GrB_Index, GrB_Info, GrB_Matrix, GrB_Semiring, GrB_Type},
};

type _Matrix = *mut DeltaMatrix;
type _MatrixTupleIter = *mut DeltaMatrixIter<'static>;

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_new(
    a: *mut _Matrix,
    ty: GrB_Type,
    nrows: GrB_Index,
    ncols: GrB_Index,
    transpose: bool,
) -> GrB_Info {
    *a = Box::into_raw(Box::new(DeltaMatrix::new(ty, nrows, ncols, transpose)));
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_getTranspose(c: _Matrix) -> _Matrix {
    match (*c).transposed() {
        Some(m) => m.as_mut() as *mut DeltaMatrix,
        None => std::ptr::null_mut(),
    }
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_M(c: _Matrix) -> GrB_Matrix {
    (&*c).m().grb_matrix_ref()
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_nrows(
    nrows: *mut GrB_Index,
    c: _Matrix,
) -> GrB_Info {
    *nrows = (&*c).nrows();
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_ncols(
    ncols: *mut GrB_Index,
    c: _Matrix,
) -> GrB_Info {
    *ncols = (&*c).ncols();
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_nvals(
    nvals: *mut GrB_Index,
    c: _Matrix,
) -> GrB_Info {
    *nvals = (&*c).nvals();
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_resize(
    c: _Matrix,
    nrows_new: GrB_Index,
    ncols_new: GrB_Index,
) -> GrB_Info {
    (*c).resize(nrows_new, ncols_new);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_setElement_BOOL(
    c: _Matrix,
    i: GrB_Index,
    j: GrB_Index,
) -> GrB_Info {
    (*c).set_element_bool(i, j);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_setElement_UINT64(
    c: _Matrix,
    x: u64,
    i: GrB_Index,
    j: GrB_Index,
) -> GrB_Info {
    (*c).set_element_u64(x, i, j);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_extractElement_BOOL(
    x: *mut bool,
    c: _Matrix,
    i: GrB_Index,
    j: GrB_Index,
) -> GrB_Info {
    if let Some(v) = (&*c).extract_element_bool(i, j) {
        if !x.is_null() {
            *x = v;
        }
        GrB_Info::GrB_SUCCESS
    } else {
        GrB_Info::GrB_NO_VALUE
    }
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_extractElement_UINT64(
    x: *mut u64,
    c: _Matrix,
    i: GrB_Index,
    j: GrB_Index,
) -> GrB_Info {
    if let Some(v) = (&*c).extract_element_u64(i, j) {
        if !x.is_null() {
            *x = v;
        }
        GrB_Info::GrB_SUCCESS
    } else {
        GrB_Info::GrB_NO_VALUE
    }
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_removeElement(
    c: _Matrix,
    i: GrB_Index,
    j: GrB_Index,
) -> GrB_Info {
    (*c).remove_element(i, j);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_removeElements(
    c: _Matrix,
    m: GrB_Matrix,
) -> GrB_Info {
    let m = From::from(m);
    (*c).remove_elements(&m);
    m.grb_matrix();
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_mxm(
    c: _Matrix,
    semiring: GrB_Semiring,
    a: _Matrix,
    b: _Matrix,
) -> GrB_Info {
    (*c).mxm(semiring, &*a, &*b);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_eWiseAdd(
    c: _Matrix,
    semiring: GrB_Semiring,
    a: _Matrix,
    b: _Matrix,
) -> GrB_Info {
    (*c).element_wise_add(semiring, &*a, &*b);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_clear(c: _Matrix) -> GrB_Info {
    (*c).clear();
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_copy(
    c: _Matrix,
    a: _Matrix,
) -> GrB_Info {
    (*c).copy(&*a);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_export(
    a: *mut GrB_Matrix,
    c: _Matrix,
) -> GrB_Info {
    *a = (&*c).export().grb_matrix();
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_pending(
    c: _Matrix,
    pending: *mut bool,
) -> GrB_Info {
    *pending = (&*c).pending();
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_wait(
    c: _Matrix,
    force_sync: bool,
) -> GrB_Info {
    (*c).wait(force_sync);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_synchronize(
    c: _Matrix,
    nrows: GrB_Index,
    ncols: GrB_Index,
) {
    (*c).synchronize(nrows, ncols);
}

#[no_mangle]
unsafe extern "C" fn Delta_Matrix_free(c: *mut _Matrix) {
    drop(Box::from_raw(c.read_unaligned()));
    c.write_unaligned(null_mut());
}

#[no_mangle]
unsafe extern "C" fn Delta_MatrixTupleIter_attach(
    iter: _MatrixTupleIter,
    a: _Matrix,
) -> GrB_Info {
    (*iter).attach(&*a);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_MatrixTupleIter_AttachRange(
    iter: _MatrixTupleIter,
    a: _Matrix,
    min_row: GrB_Index,
    max_row: GrB_Index,
) -> GrB_Info {
    (*iter).attach_range(&*a, min_row, max_row);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_MatrixTupleIter_detach(iter: _MatrixTupleIter) -> GrB_Info {
    (*iter).detach();
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_MatrixTupleIter_is_attached(
    iter: _MatrixTupleIter,
    a: _Matrix,
) -> bool {
    (&*iter).is_attached(&*a)
}

#[no_mangle]
unsafe extern "C" fn Delta_MatrixTupleIter_iterate_row(
    iter: _MatrixTupleIter,
    row_idx: GrB_Index,
) -> GrB_Info {
    (*iter).iterate_row(row_idx);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_MatrixTupleIter_iterate_range(
    iter: _MatrixTupleIter,
    start_row_idx: GrB_Index,
    end_row_idx: GrB_Index,
) -> GrB_Info {
    (*iter).iterate_range(start_row_idx, end_row_idx);
    GrB_Info::GrB_SUCCESS
}

#[no_mangle]
unsafe extern "C" fn Delta_MatrixTupleIter_next_BOOL(
    iter: _MatrixTupleIter,
    row: *mut GrB_Index,
    col: *mut GrB_Index,
    val: *mut bool,
) -> GrB_Info {
    match (*iter).next_bool() {
        Ok(Some((r, c))) => {
            if !row.is_null() {
                *row = r;
            }
            if !col.is_null() {
                *col = c;
            }
            if !val.is_null() {
                *val = true;
            }
            GrB_Info::GrB_SUCCESS
        }
        Ok(None) => GrB_Info::GxB_EXHAUSTED,
        _ => GrB_Info::GrB_NULL_POINTER,
    }
}

#[no_mangle]
unsafe extern "C" fn Delta_MatrixTupleIter_next_UINT64(
    iter: _MatrixTupleIter,
    row: *mut GrB_Index,
    col: *mut GrB_Index,
    val: *mut u64,
) -> GrB_Info {
    match (*iter).next_u64() {
        Ok(Some((r, c, v))) => {
            if !row.is_null() {
                *row = r;
            }
            if !col.is_null() {
                *col = c;
            }
            if !val.is_null() {
                *val = v;
            }
            GrB_Info::GrB_SUCCESS
        }
        Ok(None) => GrB_Info::GxB_EXHAUSTED,
        _ => GrB_Info::GrB_NULL_POINTER,
    }
}

#[no_mangle]
unsafe extern "C" fn Delta_MatrixTupleIter_reset(iter: _MatrixTupleIter) -> GrB_Info {
    (*iter).reset();
    GrB_Info::GrB_SUCCESS
}

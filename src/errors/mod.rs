use std::ffi::c_char;

// Functions that due to some limitation are still implemented in C:
extern "C" {
    pub fn ErrorCtx_RaiseRuntimeException(
        err_fmt: *const c_char,
        ...
    );

    pub fn ErrorCtx_SetError(
        err_fmt: *const c_char,
        ...
    );
}

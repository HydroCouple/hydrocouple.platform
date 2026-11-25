The OpenSWMM engine's two-component split examples, copied unchanged from
openswmm.engine examples/hydrocouple/ at bea6ef50 (Apache-2.0): split-water (a 2D surface
and a 1D network exchanging water, the engine's qualified split) and split-material (the
same pair exchanging water and pollutants, experimental in the engine's own terms).

split-material/model.inp is from 6989b454, which pins the two settings its exchange requires
(MOMENTUM_EQUATION LOCAL_INERTIAL and COUPLING_EQUALIZE NO) since FULL_SWE became the default.

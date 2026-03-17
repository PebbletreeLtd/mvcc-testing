import * as store from "./MVCCStore";
import { Transaction } from "./Transaction";
import { DerivedMVCCStore as _DerivedMVCCStore } from "./DerivedMVCCStore";
import _Subspace from "./subspace";

import * as types from "./types";

export namespace MVCCCore {
    export const MVCCStore = store.MVCCStore;
    export type MVCCStore<Kin, KOut, Vin, VOut> = store.MVCCStore<Kin, KOut, Vin, VOut>;

    export const DerivedMVCCStore = _DerivedMVCCStore;
    export type DerivedMVCCStore<Kin, KOut, Vin, VOut, FK, FVOut> = _DerivedMVCCStore<Kin, KOut, Vin, VOut, FK, FVOut>;

    export const Subspace = _Subspace;
    export type Subspace<KI, KO, VI, VO> = _Subspace<KI, KO, VI, VO>;

    export const Txn = Transaction;
    export type Txn<Kin, KOut, Vin, VOut> = Transaction<Kin, KOut, Vin, VOut>;

    export const ConflictError = types.ConflictError;

    export type TransactionOptions = types.TransactionOptions;
    export type KeyReadOperator = types.KeyReadOperation;
    export type ScanReadOperation = types.ScanReadOperation;
    export type ReadOperation = types.ReadOperation;
    export type RangeOptions = types.RangeOptions;
    export type ITransaction<Kin, KOut, Vin, VOut> = types.ITransaction<Kin, KOut, Vin, VOut>;
    export type Transformer<VIN, VOUT> = types.Transformer<VIN, VOUT>;
}

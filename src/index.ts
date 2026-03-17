import * as store from "./Store";
import { Transaction } from "./Transaction";
import { DerivedSubspace as _DerivedSubspace } from "./DerivedSubspace";
import _Subspace from "./subspace";

import * as types from "./types";

export namespace MVCCCore {
    export const Store = store.Store;
    export type Store<Kin, KOut, Vin, VOut> = store.Store<Kin, KOut, Vin, VOut>;

    export const DerivedSubspace = _DerivedSubspace;
    export type DerivedSubspace<Kin, KOut, Vin, VOut, FKIn, FKOut extends FKIn> = _DerivedSubspace<Kin, KOut, Vin, VOut, FKIn, FKOut>;

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
    export type TransactionFactory<K, V> = types.TransactionFactory<K, V>;
}

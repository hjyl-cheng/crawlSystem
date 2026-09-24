import { AsyncLocalStorage } from 'node:async_hooks';

// One hook for all Plans. Each scope still owns its transaction: independent
// Plans cannot borrow another Plan's client, including when invoked nested.
const transactions = new AsyncLocalStorage();

export function createIncrementalTransactionScope() {
  const owner = { active: true };
  return {
    getStore() {
      if (!owner.active) return undefined;
      for (let context = transactions.getStore(); context; context = context.parent) {
        if (context.owner === owner) return context.client;
      }
      return undefined;
    },
    run(client, action) {
      if (!owner.active) throw new Error('INCREMENTAL_TRANSACTION_SCOPE_CLOSED');
      return transactions.run({ owner, client, parent: transactions.getStore() }, action);
    },
    disable() {
      // Invalidates late callbacks from this Plan without disabling any other
      // Plan's context or adding/removing a global hook for every completion.
      owner.active = false;
    },
  };
}

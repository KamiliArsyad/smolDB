## The Atomicity Fuzz Testing Framework
The harness is built on a simple but powerful principle: **compare every state seen by concurrent readers against a "ground truth" model of all committed states.** If a reader ever observes a state that was never committed (i.e., it came from a transaction that later aborted), it has detected an atomicity violation.

*   **How it Works:**
    1.  **Initialization:** The harness is initialized with a set of rows (`RIDs`) to operate on. It reads their initial values and populates a `ground_truth` map, which stores the set of all valid, committed values for each `RID`.
    2.  **Thread Orchestration:** It spawns two types of threads:
        *   **Mutator Threads:** These threads run in a loop, each iteration performing a "mutation" operation (like `UPDATE` or `DELETE`) on a random `RID` within a new transaction. After the operation, the transaction is randomly committed or aborted. If committed, the `ground_truth` map is updated to include the new state.
        *   **Reader Threads:** These threads concurrently and continuously read random `RIDs`. After each successful read, they check if the value they saw is present in the `ground_truth` set for that `RID`. If not, the test fails immediately.
    3.  **Extensibility (Higher-Order Function):** The key to the harness's reusability is that the specific mutation logic is passed in as a C++ lambda function. The harness itself is agnostic to *what* the mutation is; it only cares about orchestrating the test and validating the outcome.

*   **Future Use-Cases:**
    *   **Testing `INSERT`:** The mutator could randomly `INSERT` a new row and `DELETE` an existing one. The `ground_truth` map would be updated to track the existence of new RIDs.
    *   **Testing Multi-Row Atomicity:** The mutator lambda could update two rows within a single transaction. The reader threads would then need to be adapted to read both rows and verify that they only ever see `(old_A, old_B)` or `(new_A, new_B)`, but never `(new_A, old_B)`.
    *   **Testing Different Isolation Levels:** If we were to implement other isolation levels like Read Committed, this harness would be the perfect tool to verify that dirty reads are prevented while non-repeatable reads are allowed.
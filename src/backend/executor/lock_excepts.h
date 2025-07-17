#ifndef LOCK_EXCEPTS_H
#define LOCK_EXCEPTS_H
#include <stdexcept>

/**
 * @brief Base class for exceptions that cause a transaction to abort but may be
 * retryable.
 */
class TransactionAbortedException : public std::runtime_error
{
public:
  using std::runtime_error::runtime_error;
};

class LockTimeoutException : public TransactionAbortedException
{
public:
  LockTimeoutException() : TransactionAbortedException("Lock wait timeout") {}
};

class DeadlockException : public TransactionAbortedException
{
public:
  DeadlockException() : TransactionAbortedException("Deadlock detected") {}
};

#endif //LOCK_EXCEPTS_H

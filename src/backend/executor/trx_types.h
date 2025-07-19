#ifndef TRX_TYPES_H
#define TRX_TYPES_H
#pragma once
#include <cstdint>

namespace smoldb
{

// A unique identifier for a transaction.
using TransactionID = uint64_t;
constexpr TransactionID INVALID_TXN_ID = 0;

}  // namespace smoldb
#endif  // TRX_TYPES_H

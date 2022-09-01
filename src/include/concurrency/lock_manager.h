//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>
#include <stack>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
  enum class LockMode { SHARED, EXCLUSIVE };
  enum class VisitedType { NOT_VISITED, IN_STACK, VISITED };

  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    txn_id_t txn_id_;
    LockMode lock_mode_;
    bool granted_;
  };

  class LockRequestQueue {
   public:
    std::mutex latch_;
    std::list<LockRequest> request_queue_;
    // for notifying blocked transactions on this rid
    std::condition_variable cv_;
    // txn_id of an upgrading transaction (if any)
    txn_id_t upgrading_ = INVALID_TXN_ID;
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock prevention policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
    LOG_INFO("Cycle detection thread launched");
  }

  explicit LockManager(bool enable_cycle_detection) {
    enable_cycle_detection_ = enable_cycle_detection;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
    if (enable_cycle_detection) {
      LOG_INFO("Cycle detection thread launched");
    }
  }

  ~LockManager() {
    bool enable_cycle_detection = enable_cycle_detection_;
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join(); //有点问题，永远阻塞不会销毁？
    delete cycle_detection_thread_;
    if (enable_cycle_detection) {
      LOG_INFO("Cycle detection thread stopped");
    }
  }

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the
   * same transaction, i.e. the transaction is responsible for keeping track of
   * its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  auto LockShared(Transaction *txn, const RID &rid) -> bool;

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  auto LockExclusive(Transaction *txn, const RID &rid) -> bool;

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the
   * requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockUpgrade(Transaction *txn, const RID &rid) -> bool;

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the
   * lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  auto Unlock(Transaction *txn, const RID &rid) -> bool;
  
  /** Adds an edge from t1 -> t2. */
  void AddEdge(txn_id_t t1, txn_id_t t2);

  /** Removes an edge from t1 -> t2. */
  void RemoveEdge(txn_id_t t1, txn_id_t t2);

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  bool HasCycle(txn_id_t *txn_id);

  /** @return the set of all edges in the graph, used for testing only! */
  std::vector<std::pair<txn_id_t, txn_id_t>> GetEdgeList();

  /** Runs cycle detection in the background. */
  void RunCycleDetection();

  /**
   * Test lock compatibility for a lock request against the lock request queue that it accquires lock on
   *
   * Return true if and only if:
   * - queue is empty
   * - compatible with locks that are currently held
   * - all **earlier** requests have been granted already
   * @param lock_request_queue the queue to test compatibility
   * @param lock_request the request to test
   * @return true if compatible, otherwise false
   */
  static bool IsLockCompatible(const LockRequestQueue &lock_request_queue, const LockRequest &to_check_request) {
    for (auto &&lock_request : lock_request_queue.request_queue_) {
      if (lock_request.txn_id_ == to_check_request.txn_id_) {
        return true;
      }

      const auto isCompatible =
          lock_request.granted_ &&                         // all **earlier** requests have been granted already
          (lock_request.lock_mode_ == LockMode::EXCLUSIVE  // compatible with locks that are currently held
               ? false
               : to_check_request.lock_mode_ != LockMode::EXCLUSIVE);
      if (!isCompatible) {
        return false;
      }
    }

    return true;
  }

  void StopCycleDetection() { enable_cycle_detection_ = false; }
 private:

  void AbortImplicitly(Transaction *txn, AbortReason abort_reason);
  bool ProcessDFSTree(txn_id_t *txn_id, std::stack<txn_id_t> *stack,
                      std::unordered_map<txn_id_t, VisitedType> *visited);
  txn_id_t GetYoungestTransactionInCycle(std::stack<txn_id_t> *stack, txn_id_t vertex);
  void BuildWaitsForGraph();

  std::mutex latch_;
  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
};

}  // namespace bustub

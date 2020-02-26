class Connection:
    def __init__(self):
        self.xid = 0

    def _start_transaction(self, read_only=True):
        print('starting transaction', self.xid)
        rslt = self.xid
        self.xid = self.xid + 1
        return rslt

    def _commit_transaction(self, xid):
        print('committing transaction', xid)

    def _rollback_transaction(self, xid):
        print('rolling back transaction', xid)

class Transaction:
    def __init__(self, conn, read_only=True):
        self.conn = conn
        self.xid = conn._start_transaction(read_only=read_only)

    def commit(self):
        self.conn._commit_transaction(self.xid)

    def rollback(self):
        self.conn._rollback_transaction(self.xid)

# @contextlib.contextmanager
# def start_transaction(connection, read_only=True):
#     xid = connection.start_transaction(read_only=read_only)
#     tx = Transaction(connection, xid)

#     try:
#         yield tx
#     except Exception:
#         tx.rollback()
#         raise

#     tx.commit()

package com.ztesoft.zsmart.zmq.common.sysflag;

public class MessageSysFlag {
	/**
	 * SysFlag
	 */
	public final static int CompressedFlag = (0x1 << 0);//1
	public final static int MultiTagsFlag = (0x1 << 1);//10

	/**
	 * 7 6 5 4 3 2 1 0<br>
	 * SysFlag 事务相关，从左属，2与3
	 */
	public final static int TransactionNotType = (0x0 << 2); // 0
	public final static int TransactionPreparedType = (0x1 << 2); //4
	public final static int TransactionCommitType = (0x2 << 2); //1000 8
	public final static int TransactionRollbackType = (0x3 << 2);//1100 12

	public static int getTransactionValue(final int flag) {
		return flag & TransactionRollbackType;
	}

	public static int resetTransactionValue(final int flag, final int type) {
		return (flag & (~TransactionRollbackType)) | type;
	}

	public static int clearCompressedFlag(final int flag) {
		return flag & (~CompressedFlag);
	}
}

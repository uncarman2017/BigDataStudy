package org.redmaplesoft.bigdata.hadoopstudy.lesson9.sort.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//代表员工
//数据：7654,MARTIN,SALESMAN,7698,1981/9/28,1250,1400,30
public class Emp implements WritableComparable<Emp> {
    //员工号
    private int empno;
    //员工姓名
    private String ename;
    //职位
    private String job;
    //经理的员工号
    private int mgr;
    //入职日期
    private String hiredate;
    //月薪
    private int sal;
    //奖金
    private int comm;
    //部门号
    private int deptno;

//	@Override
//	public int compareTo(Emp o) {
//		// 定义自己的排序规则：一个列的排序
//		// 按照薪水进行排序
//		if(this.sal >= o.getSal()){
//			return 1;
//		}else{
//			return -1;
//		}
//	}

    @Override
    public int compareTo(Emp o) {
        // 定义自己的排序规则：多个列的排序
        // 先按照部门号进行排序，再按照薪水进行排序
        if (this.deptno > o.getDeptno()) {
            return 1;
        } else if (this.deptno < o.getDeptno()) {
            return -1;
        }

        //再按照薪水进行排序
        if (this.sal >= o.getSal()) {
            return 1;
        } else {
            return -1;
        }
    }

    @Override
    public String toString() {
        return "Emp [empno=" + empno + ", ename=" + ename + ", sal=" + sal + ", deptno=" + deptno + "]";
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        //实现反序列化，从输入流中读取对象
        this.empno = input.readInt();
        this.ename = input.readUTF();
        this.job = input.readUTF();
        this.mgr = input.readInt();
        this.hiredate = input.readUTF();
        this.sal = input.readInt();
        this.comm = input.readInt();
        this.deptno = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // 实现序列化，把对象输出到输出流
        output.writeInt(this.empno);
        output.writeUTF(this.ename);
        output.writeUTF(this.job);
        output.writeInt(this.mgr);
        output.writeUTF(this.hiredate);
        output.writeInt(this.sal);
        output.writeInt(this.comm);
        output.writeInt(this.deptno);
    }


    public int getEmpno() {
        return empno;
    }

    public void setEmpno(int empno) {
        this.empno = empno;
    }

    public String getEname() {
        return ename;
    }

    public void setEname(String ename) {
        this.ename = ename;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public int getMgr() {
        return mgr;
    }

    public void setMgr(int mgr) {
        this.mgr = mgr;
    }

    public String getHiredate() {
        return hiredate;
    }

    public void setHiredate(String hiredate) {
        this.hiredate = hiredate;
    }

    public int getSal() {
        return sal;
    }

    public void setSal(int sal) {
        this.sal = sal;
    }

    public int getComm() {
        return comm;
    }

    public void setComm(int comm) {
        this.comm = comm;
    }

    public int getDeptno() {
        return deptno;
    }

    public void setDeptno(int deptno) {
        this.deptno = deptno;
    }

}

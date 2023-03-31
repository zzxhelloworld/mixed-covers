package entity;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import util.Utils;
/**
 * functional dependency
 * format:
 * leftHand -> rightHand
 */
public class FD {
	private List<String> leftHand;
	private List<String> rightHand;
	private int n_key;//key number, for fd X -> A,use union of left and right as schema, Sigma[XA] as FD set, then get key number on that
	private int level;//Level of Cardinality Constraint of lhs(update inefficiency/join efficiency) over original data set
	private int subschema_level;//subschema level over original data set that subschema refers to all attributes of FD
	private int non_key_fd_num;//give a schema with only attributes from the FD, the schema's non-key FD number
	
	public FD() {
		
	}
	
	public FD(List<String> leftHand, List<String> rightHand) {
		this.leftHand = leftHand;
		this.rightHand = rightHand;
		this.n_key = 0;
		this.level = 0;
		this.subschema_level = 0;
		this.non_key_fd_num = 0;
	}
	
	public FD(List<String> leftHand, List<String> rightHand, Boolean sortByNum) {
		if(sortByNum) {
			this.leftHand = Utils.sortByNumbers(leftHand);
			this.rightHand = Utils.sortByNumbers(rightHand);
		}else {
			this.leftHand = leftHand;
			this.rightHand = rightHand;
		}
		this.n_key = 0;
		this.level = 0;
		this.subschema_level = 0;
		this.non_key_fd_num = 0;
	}
	
	public FD(List<String> leftHand, List<String> rightHand,int level,int n_key) {
		this.leftHand = leftHand;
		this.rightHand = rightHand;
		this.level = level;
		this.n_key = n_key;
		this.subschema_level = 0;
		this.non_key_fd_num = 0;
	}

	public List<String> getLeftHand() {
		return leftHand;
	}

	public void setLeftHand(List<String> leftHand) {
		this.leftHand = leftHand;
	}

	public List<String> getRightHand() {
		return rightHand;
	}

	public void setRightHand(List<String> rightHand) {
		this.rightHand = rightHand;
	}
	
	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}
	
	public int getN_key() {
		return n_key;
	}

	public void setN_key(int n_key) {
		this.n_key = n_key;
	}

	public int getSubschema_level() {
		return subschema_level;
	}

	public void setSubschema_level(int subschema_level) {
		this.subschema_level = subschema_level;
	}

	public int getNon_key_fd_num() {
		return non_key_fd_num;
	}

	public void setNon_key_fd_num(int non_key_fd_num) {
		this.non_key_fd_num = non_key_fd_num;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof FD) {
			FD fd = (FD)obj;
			if(fd.getLeftHand().containsAll(this.leftHand) && fd.getLeftHand().size() == this.leftHand.size() &&
					fd.getRightHand().containsAll(this.rightHand) && fd.getRightHand().size() == this.rightHand.size())
				return true;
			else
				return false;
				
		}else
			return false;
	}
	
	

	@Override
	public String toString() {
		return "FD " + leftHand + " -> " + rightHand;
	}

	@Override
	public int hashCode() {
		return Objects.hash(new HashSet<String>(leftHand), new HashSet<String>(rightHand));
	}

	
	
}

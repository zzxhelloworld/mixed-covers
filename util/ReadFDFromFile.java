package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import entity.FD;

public class ReadFDFromFile {
	
	public static void restoreFDsToJsonFile(String input, String output, int schemaSize) {
		List<String> txt = Utils.readContent(input);
		List<FD> FDs = new ArrayList<>();
		for(String line : txt) {
			if(line.contains("->")) {
				String[] fd = line.split("->");
				String[] left = fd[0].split(",");
				String[] right = fd[1].split(",");
				List<String> leftHand = new ArrayList<>();
				List<String> rightHand = new ArrayList<>();
				for(String a : left) {
					leftHand.add((Integer.parseInt(a)-1)+"");
				}
				for(String a : right) {
					rightHand.add((Integer.parseInt(a)-1)+"");
				}
				FDs.add(new FD(leftHand, rightHand));
			}
		}
		Map<Set<String>, FD> map = new HashMap<>();//key = left hand, value = fd has the LHS
		for(FD fd : FDs) {
			Set<String> lhs = new HashSet<>(fd.getLeftHand());
			if(map.containsKey(lhs)) {
				FD f = map.get(lhs);
				for(String a : fd.getRightHand()) {
					if(!f.getRightHand().contains(a)) {
						f.getRightHand().add(a);
					}
				}
			}else {
				map.put(lhs, fd);
			}
		}
		List<FD> res = new ArrayList<>();
		for(Map.Entry<Set<String>, FD> e : map.entrySet()) {
			res.add(e.getValue());
		}
		
		Utils.writeFDs(schemaSize, res, output);
	}
	public static void main(String[] args) {
		String input = "C:\\Users\\wang\\Desktop\\phd论文\\CONF的相关工作\\3NF\\dataset\\letterFDs(non-dup).txt";
		String output = "C:\\Users\\wang\\Desktop\\phd论文\\CONF的相关工作\\3NF\\dataset\\letter(non-dup).json";
		ReadFDFromFile.restoreFDsToJsonFile(input, output, 17);
	}

}

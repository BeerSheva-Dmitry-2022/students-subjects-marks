package telran.students.entities;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "students")
public class StudentDoc {
	@Id
	int stid;
	List<SubjectMark> marks = new ArrayList<>();
	String name;
	public StudentDoc(int stid, String name) {
		this.stid = stid;
		this.name = name;
	}
	public int getStid() {
		return stid;
	}
	public List<SubjectMark> getMarks() {
		return marks;
	}
	public String getName() {
		return name;
	}
	@Override
	public String toString() {
		return "StudentDoc [stid=" + stid + ", marks=" + marks + ", name=" + name + "]";
	}

}

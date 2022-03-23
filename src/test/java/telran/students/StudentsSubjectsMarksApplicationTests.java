package telran.students;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.*;

import java.io.UnsupportedEncodingException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.EmbeddedDatabaseConnection;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;

import telran.students.dto.Mark;
import telran.students.dto.Student;
import telran.students.dto.Subject;
import telran.students.service.interfaces.*;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = "spring.mongodb.embedded.version=4.0.2")
//@AutoConfigureTestDatabase(connection = EmbeddedDatabaseConnection.H2)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StudentsSubjectsMarksApplicationTests {
	ObjectMapper mapper = new ObjectMapper();
	@Autowired
	MockMvc mockMvc;
	@Autowired
	StudentsService studentsService;
	@Test
	void contextLoads() {
		assertNotNull(mockMvc);
	}
	@Test
	@Order(1)
	void dbload() {
		studentsService.addStudent(new Student(1, "Moshe"));
		studentsService.addStudent(new Student(2, "Sara"));
		studentsService.addStudent(new Student(3, "Vasya"));
		studentsService.addStudent(new Student(4, "Olya"));
		studentsService.addSubject(new Subject(1, "React"));
		studentsService.addSubject(new Subject(2, "Java"));
		studentsService.addMark(new Mark(1, 1, 90));
		studentsService.addMark(new Mark(1, 2, 90));
		studentsService.addMark(new Mark(2, 1, 80));
		studentsService.addMark(new Mark(2, 2, 80));
		studentsService.addMark(new Mark(3, 2, 40));
		studentsService.addMark(new Mark(4, 2, 45));

	}
	
	@Test
	@Order(95)
	void worstMarks() throws Exception  {
		String subject = "Java";
		String name = "Vasya";
		int mark = 40;
		
		testForWorstMarks(subject, name, mark);
	}
	
	@Test
	@Order(100)
	void deleteStudents() throws Exception {
		String resJson = mockMvc.perform(MockMvcRequestBuilders.delete("/students/delete?avgMark=45&nMarks=2"))
				.andReturn().getResponse().getContentAsString();
		Student[] students = mapper.readValue(resJson, Student[].class);
		Student[] expected = {new Student(3, "Vasya")};
		assertArrayEquals(students, expected);
	}
	
	private void testForWorstMarks(String subject, String name, int mark)
			throws UnsupportedEncodingException, Exception, JsonProcessingException, JsonMappingException {
		String resJson = mockMvc.perform(MockMvcRequestBuilders.get("/students/worst/marks?amount=1"))
				.andReturn().getResponse().getContentAsString();
		StSuMark[] subMarks = mapper.readValue(resJson, StSuMark[].class);
		testWorstMarks(subMarks, subject, name, mark);
	}
	
	private void testWorstMarks(StSuMark[] subMarks, String subject, String name, int mark) {
		assertEquals(1, subMarks.length);
		assertEquals(subject, subMarks[0].subjectSubject);
		assertEquals(name, subMarks[0].studentName);
		assertEquals(mark, subMarks[0].mark);
	}
	
	@Test
	@Order(99)
	void bestStudents() throws Exception {
		String resJson = mockMvc.perform(MockMvcRequestBuilders.get("/students/best"))
				.andReturn().getResponse().getContentAsString();
		String[] res = mapper.readValue(resJson, String[].class);
		assertEquals(2, res.length);
		assertTrue(res[0].contains("Moshe"));
		assertTrue(res[1].contains("Sara"));
	}
	
	@Test
	@Order(98)
	void bestTopStudents() throws Exception {
		String resJson = mockMvc.perform(MockMvcRequestBuilders.get("/students/best?amount=1"))
				.andReturn().getResponse().getContentAsString();
		String[] res = mapper.readValue(resJson, String[].class);
		assertEquals(1, res.length);
		assertTrue(res[0].contains("Moshe"));
	}
	
}

class StSuMark {
	public String subjectSubject;
	public String studentName;
	public int mark;
}

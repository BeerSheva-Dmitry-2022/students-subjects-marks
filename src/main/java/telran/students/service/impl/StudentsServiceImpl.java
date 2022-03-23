package telran.students.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.security.auth.SubjectDomainCombiner;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.mongodb.internal.operation.AggregateOperation;

import telran.students.dto.*;
import telran.students.entities.*;
import telran.students.repo.*;
import telran.students.service.interfaces.*;

@Service
public class StudentsServiceImpl implements StudentsService {

	private static final int MAX_MARK = 100;
	private static final int MIN_MARK = 60;
	StudentsRepository studentsRepository;
	SubjectsRepository subjectsRepository;
	MongoTemplate mongoTemplate;

	public StudentsServiceImpl(StudentsRepository studentsRepository, SubjectsRepository subjectsRepository,
			MongoTemplate mongoTemplate) {
		this.studentsRepository = studentsRepository;
		this.subjectsRepository = subjectsRepository;
		this.mongoTemplate = mongoTemplate;
	}

	@Override
	public void addStudent(Student student) {
		studentsRepository.save(new StudentDoc(student.stid, student.name));
	}

	@Override
	public void addSubject(Subject subject) {
		subjectsRepository.save(new SubjectDoc(subject.suid, subject.subject));

	}

	@Override
	public Mark addMark(Mark mark) {
		StudentDoc student = studentsRepository.findById(mark.stid).orElse(null);
		if (student == null) {
			return null;
		}
		SubjectDoc subject = subjectsRepository.findById(mark.suid).orElse(null);
		if (subject == null) {
			return null;
		}
		student.getMarks().add(new SubjectMark(subject.getSubject(), mark.mark));
		studentsRepository.save(student);
		return mark;
	}

	@Override
	public List<StudentSubjectMark> getMarksStudentSubject(String name, String subject) {
		StudentDoc student = studentsRepository.findByName(name);
		if (student == null) {
			return Collections.emptyList();
		}

		return getStudentsStream(name, subject, student).toList();
	}

	private Stream<StudentSubjectMark> getStudentsStream(String name, String subject, StudentDoc student) {
		return student.getMarks().stream().filter(p -> p.getSubject().equals(subject))
				.map(sm -> getStudentSubjectMark(sm, name));
	}

	private StudentSubjectMark getStudentSubjectMark(SubjectMark sm, String name) {

		return new StudentSubjectMark() {

			@Override
			public String getSubjectSubject() {
				return sm.getSubject();
			}

			@Override
			public String getStudentName() {
				return name;
			}

			@Override
			public int getMark() {
				return sm.getMark();
			}
		};
	}

	@Override
	public List<String> getBestStudents() {
		List<AggregationOperation> listOperations = getStudentAvgMark(Direction.DESC);
		double avgCollegeMark = getAvgCollegeMark();
		MatchOperation matchOperation = Aggregation.match(Criteria.where("avgMark").gt(avgCollegeMark));
		listOperations.add(matchOperation);
		return resultProcessing(listOperations, true);
	}

	@Override
	public List<String> getTopBestStudents(int nStudents) {
		List<AggregationOperation> listOperation = getStudentAvgMark(Direction.DESC);
		LimitOperation limit = Aggregation.limit(nStudents);
		listOperation.add(limit);
		return resultProcessing(listOperation, true);
	}

	private List<String> resultProcessing(List<AggregationOperation> listOperation, boolean markInOutput) {
		try {
			List<Document> documentsRes = mongoTemplate
					.aggregate(Aggregation.newAggregation(listOperation), StudentDoc.class, Document.class)
					.getMappedResults();

			return documentsRes.stream().map(
					doc -> doc.getString("_id") + (markInOutput ? ("," + doc.getDouble("avgMark").intValue()) : ""))
					.toList();
		} catch (Exception e) {
			ArrayList<String> errorMessage = new ArrayList<>();
			errorMessage.add(e.getMessage());
			return errorMessage;
		}
	}

	private List<AggregationOperation> getStudentAvgMark(Direction sortDirection) {
		UnwindOperation unwindOperation = Aggregation.unwind("marks");
		GroupOperation groupOperation = Aggregation.group("name").avg("marks.mark").as("avgMark");
		SortOperation sortOperation = Aggregation.sort(sortDirection, "avgMark");

		return new ArrayList<>(Arrays.asList(unwindOperation, groupOperation, sortOperation));
	}

	private double getAvgCollegeMark() {
		UnwindOperation unwindOperation = Aggregation.unwind("marks");
		GroupOperation groupOperation = Aggregation.group().avg("marks.mark").as("avgMark");
		Aggregation pipeline = Aggregation.newAggregation(unwindOperation, groupOperation);
		return mongoTemplate.aggregate(pipeline, StudentDoc.class, Document.class).getUniqueMappedResult()
				.getDouble("avgMark");
	}

	@Override
	public List<Student> getTopBestStudentsSubject(int nStudents, String subject) {
		List<AggregationOperation> listOperations = getStudentSubjectAvgMark(subject, nStudents);
		//Aggregation pipeline = Aggregation.newAggregation(listOperations);
		List<Document> documents = mongoTemplate
				.aggregate(Aggregation.newAggregation(listOperations), StudentDoc.class, Document.class)
				.getMappedResults();
		return documents.stream().map(this::getStudentFromDoc).toList();
	}

	private Student getStudentFromDoc(Document doc) {
		Document groupID = (Document) doc.get("_id");
		return new Student(groupID.getInteger("stid"), groupID.getString("name"));
	}

	private List<AggregationOperation> getStudentSubjectAvgMark(String subject, int nStudents) {

		UnwindOperation unwindOperation = Aggregation.unwind("marks");
		MatchOperation matchOperation = Aggregation.match(Criteria.where("marks.subject").is(subject));
		GroupOperation groupOperation = Aggregation.group("stid", "name").avg("marks.mark").as("avgMark");
		SortOperation sortOperation = Aggregation.sort(Direction.DESC, "avgMark");
		LimitOperation limit = Aggregation.limit(nStudents);

		return new ArrayList<>(Arrays.asList(unwindOperation, matchOperation, groupOperation, sortOperation, limit));
	}

	@Override
	public List<StudentSubjectMark> getMarksOfWorstStudents(int nStudents) {
		List<AggregationOperation> listOperation = getStudentAvgMark(Direction.ASC);
		LimitOperation limit = Aggregation.limit(nStudents);
		listOperation.add(limit);
//		List<Document> documentsRes = mongoTemplate
//				.aggregate(Aggregation.newAggregation(listOperation), StudentDoc.class, Document.class)
//				.getMappedResults();
		List<String> names = resultProcessing(listOperation, false);
		List<StudentDoc> students = studentsRepository.findByNameIn(names);
//		List<String> list = documentsRes.stream().map(doc -> doc.getString("_id")).toList();
//		List<StudentDoc> students = studentsRepository.findByNameIn(list);

		return students.stream()
				.flatMap(student -> student.getMarks().stream().map(sm -> getStudentSubjectMark(sm, student.getName())))
				.toList();
	}

	@Override
	public List<IntervalMarks> markDistibution(int interval) {
		int nInterval = (MAX_MARK-MIN_MARK)/interval;
		UnwindOperation unwindOperation = Aggregation.unwind("marks");
		BucketAutoOperation bucketAutoOperation = Aggregation.bucketAuto("marks.mark", nInterval);
		List<Document> bucketDocs = mongoTemplate
				.aggregate(Aggregation.newAggregation(unwindOperation, bucketAutoOperation), StudentDoc.class, Document.class).getMappedResults();
		return bucketDocs.stream().map(this::getIntervalMarks).toList();
	}

	private IntervalMarks getIntervalMarks(Document doc) {
		return new IntervalMarks() {
			Document intervalDoc = (Document) doc.get("_id");
			@Override
			public int getOccurrences() {
				return doc.getInteger("count");
			}
			
			@Override
			public int getMin() {
				return intervalDoc.getInteger("min");
			}
			
			@Override
			public int getMax() {
				return intervalDoc.getInteger("max");
			}
		};
	}

	@Override
	public List<String> jpqlQuery(String jpql) {
		ArrayList<String> res = new ArrayList<>();
		res.add("JPQL is not supported ");
		return res;
	}

	@Override
	public List<String> nativQuery(String queryJson) {
		List<StudentDoc> res;
		try {
			BasicQuery query = new BasicQuery(queryJson);
			res = mongoTemplate.find(query, StudentDoc.class);
			return res.stream().map(StudentDoc::toString).toList();
		} catch (Exception e) {
			ArrayList<String> errorMessage = new ArrayList<>();
			errorMessage.add(e.getMessage());
			return errorMessage;
		}
	}

	@Override
	@Transactional
	public List<Student> removeStudents(int avgMark, int nMarks) {
		UnwindOperation unwindOperation = Aggregation.unwind("marks");
		GroupOperation groupOperation = Aggregation.group("stid", "name").avg("marks.mark").as("avgMark").count().as("count");
		MatchOperation matchOperation = Aggregation.match(Criteria.where("avgMark").lt((double)avgMark).and("count").lt((long)nMarks));
		List<Document> documents = mongoTemplate
				.aggregate(Aggregation.newAggregation(unwindOperation, groupOperation, matchOperation), StudentDoc.class, Document.class)
				.getMappedResults();
		List<Student> studentsForRemoving = documents.stream().map(this::getStudentFromDoc).toList();
		studentsRepository.deleteAllById(studentsForRemoving.stream().map(s -> s.stid).toList());
		return studentsForRemoving;

	}

}

package edu.javacourse.studentorder.validator;

import edu.javacourse.studentorder.domain.student.AnswerStudent;
import edu.javacourse.studentorder.domain.StudentOrder;

public class StudentValidator {

    public AnswerStudent checkStudent(StudentOrder studentOrder) {
        System.out.println("checkStudent is running");
        AnswerStudent answerStudent = new AnswerStudent();
        return answerStudent;
    }
}

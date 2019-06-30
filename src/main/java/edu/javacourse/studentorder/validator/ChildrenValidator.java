package edu.javacourse.studentorder.validator;

import edu.javacourse.studentorder.domain.children.AnswerChildren;
import edu.javacourse.studentorder.domain.StudentOrder;

public class ChildrenValidator {

    public AnswerChildren checkChildren(StudentOrder studentOrder) {
        System.out.println("checkChildren is running");
        AnswerChildren answerChildren = new AnswerChildren();
        return answerChildren;
    }
}

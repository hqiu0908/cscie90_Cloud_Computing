package edu.hu.lambda;

import com.amazonaws.services.lambda.runtime.Context; 

public class Hello2 {

    public static class RequestClass {
        String myString;

        public String getmyString() {
            return myString;
        }

        public void setmyString(String myString) {
            this.myString = myString;
        }

        public RequestClass(String myString) {
            this.myString = myString;
        }

        public RequestClass() {
        }
    }

    public static class ResponseClass {
        String myString;

        public String getmyString() {
            return myString;
        }

        public void setmyString(String myString) {
            this.myString = myString;
        }

        public ResponseClass(String myString) {
            this.myString = myString;
        }

        public ResponseClass() {
        }

    }

    public static ResponseClass myHandler(RequestClass request, Context context){
        String myStringtring = String.format("%s%s", request.myString, request.myString);
        return new ResponseClass(myStringtring);
    }
}
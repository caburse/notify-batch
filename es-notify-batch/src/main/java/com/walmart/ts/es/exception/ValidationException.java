package com.walmart.ts.es.exception;

public class ValidationException extends Exception{

		private static final long serialVersionUID = 1L;
		
		private String errorCode;
		
		private String errorMessage;
		
		
		/**
		 * @return the code
		 */
		public final String getErrorCode() {
			return errorCode;
		}

		/**
		 * @param code the code to set
		 */
		public final void setErrorCode(String code) {
			this.errorCode = code;
		}

		/**
		 * @return the message
		 */
		public final String getErrorMessage() {
			return errorMessage;
		}

		/**
		 * @param message the message to set
		 */
		public final void setErrorMessage(String message) {
			this.errorMessage = message;
		}
		
		/**
		 * 
		 */
		public ValidationException(){
			super();
		}
		
		
		/**
		 * @param cd
		 * @param msg
		 */
		public ValidationException(String cd, String msg){
			super();
			errorCode = cd;
			errorMessage = msg;
		}
		
		/**
		 * @param message
		 * @param error
		 */
		public  ValidationException(String message, Throwable error){
			super(message, error);
		}
		/**
		 * @param message
		 */
		public  ValidationException(String message){
			super(message);
		}
		
		/**
		 * @param e
		 */
		public  ValidationException(Exception e){
			super(e);
		}
}

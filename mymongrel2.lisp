(declaim (optimize (speed 3)))
;(declaim (optimize (debug 3)))
(in-package :mymongrel2)

(eval-when (:compile-toplevel :load-toplevel :execute) (defvar *con-fun-list* nil))


(defconstant +deliver-max+ 128)
(defvar *current-connection* nil)

(defparameter *zmq-context* nil)

(defun ensure-zmq-init (&optional (threads 1))
    (unless *zmq-context* (setq *zmq-context* (zmq:init threads))))

(defun zmq-recv-string (socket)
  (let ((query (make-instance 'zmq:msg))
	(cffi:*default-foreign-encoding* :iso-8859-1))
    (zmq:recv socket query)
    (zmq:msg-data-as-string query)))

(defparameter +crlf+ "
")

(defun format-chunk (stream &rest rest)
  (apply #'format stream 
	  "~X
~A
" rest))

(defun format-http (stream &rest rest)
  (apply #'format stream 
  "HTTP/1.1 ~A ~A
~:{~A: ~A
~}
~A" rest))

(defun myjson-decode (s)
  (let ((json:*lisp-identifier-name-to-json* (lambda (x) x))
	(json:*json-identifier-name-to-lisp* (lambda (x) x)))
    (json:decode-json-from-string s)))


(defmacro with-connection ((conn &rest init-args)
                           &body body)
  "Creates a connection and executes body within it.  Also rebinds all
   functions that take a symbol to take the connection implicitly"
     `(let ((,conn (make-connection ,@init-args)))
          (unwind-protect
            (let ((*current-connection* ,conn))
	      ,@body)
            (close-connection ,conn))))

(defmacro with-connection-nowrap ((conn &rest init-args)
                           &body body)
  "Creates a connection and executes body within it.
   Basically, all your mongrel2 stuff should be wrapped in this."
    `(let ((,conn (make-connection ,@init-args)))
       (unwind-protect
	    (progn ,@body)
	 (close-connection ,conn))))

;Sadly this is way faster on sbcl then the simpler
;(map 'string #'code-char bytes)
(defun bytes-to-string (bytes)
  (declare (type (simple-array (unsigned-byte 8) (*)) bytes))
  (let* ((length (length bytes))
         (s (make-string length)))
    (dotimes (i length)
      (setf (aref s i) (code-char (aref bytes i))))
    s))


(defun http-response (body code status headers)
  "Generates an http response; used internally"
  (declare (type vector body))
  (let ((headers (acons "Content-Length" (length body) headers))
        (body (if (typep body 'string)
                body
                (bytes-to-string body))))
    (format-http nil code status
	    (mapcar (lambda (x) (list (car x) (cdr x))) headers)
	    body)))

(defun string-join (separator sequence)
  "Basically the equivalent to pythons string.join
  Joins a sequence of strings with a separator string"
  (apply #'concatenate 'string
	 (cons (car sequence)
	       (loop for i in (cdr sequence)
		    collect separator
		    collect i))))

(defstruct request
	   (sender)
	   (conn-id)
	   (path)
	   (headers)
	   (body))


(defun request-disconnectp (req)
  "Returns true if a request should be followed by a disconnect"
  (and (equalp (cdr (assoc :METHOD (request-headers req))) "JSON")
       (equalp (cdr (assoc :|type| (myjson-decode (request-body req)))) "disconnect")))


(defun read-to-space (string pos)
  (declare (type simple-string string)
	   (type fixnum pos))
  (values (with-output-to-string (out)
	    (loop for c = (aref string pos)
	       do (incf pos)
	       while (not (eql c #\Space))
	       do (write-char c out))) pos))


;TODO start using metabang-bind?
(defun parse (msg)
  "Parses out the mongrel2 message format which is:
  UUID ID PATH SIZE:HEADERS,SIZE:BODY,"
  (declare (type simple-string msg))
  (multiple-value-bind (sender pos) (read-to-space msg 0)
    (multiple-value-bind (conn-id pos) (read-to-space msg pos)
      (multiple-value-bind (path pos) (read-to-space msg pos)
	(multiple-value-bind (headers pos) (tnetstring:parse-tnetstring msg pos)
	  (let ((headers (if (typep headers 'string)
			    (myjson-decode headers)
			    headers))
		(body (tnetstring:parse-tnetstring msg pos)))
	(make-request :sender sender
		      :conn-id conn-id
		      :path path
		      :headers headers
		      :body body)))))))

(defclass connection ()
  ((sender-id  :initarg :sender-id)
   (sub-addr :initarg :sub-addr)
   (pub-addr :initarg :pub-addr)
   (reqs :initarg :reqs)
   (resp :initarg :resp)))

(defun make-connection (sender-id sub-addr pub-addr)
  "Makes a new connection object.  Consider using with-connection instead"
  (ensure-zmq-init)
  (let* ((reqs (zmq:socket *zmq-context* zmq:PULL))
	 (resp (zmq:socket *zmq-context* zmq:PUB))
	 (connection (make-instance 'connection
				    :sender-id sender-id
				    :sub-addr sub-addr
				    :pub-addr pub-addr
				    :reqs reqs
				    :resp resp)))
    (zmq:connect reqs sub-addr)
    (handler-bind ((t (lambda (x) (zmq:close reqs) (signal x))))
	   (zmq:setsockopt resp zmq:IDENTITY sender-id)
	   (zmq:connect resp pub-addr))
    connection))

(defun close-connection (c)
  "Closes the connection given to it."
  (zmq:close (slot-value c 'reqs))
  (zmq:close (slot-value c 'resp)))


(defun recv (&optional (connection *current-connection*))
  "Receives a single message from mongrel2"
  (declare (type connection connection))
  (parse (zmq-recv-string (slot-value connection 'reqs))))

(defun recv-json (&optional (connection *current-connection*))
  "Receives a single message from mongrel2 and decodes as JSON"
  (declare (type connection connection))
  (json:decode-json-from-string (recv connection)))

(defun send (uuid conn-id msg &optional (connection *current-connection*))
  "Sends a single message to mongrel2"
  (declare (type connection connection))
  (declare (type vector conn-id msg))
  ;TODO can optimize by eliminating a copy here
  ;
  (let* ((fmsg (format nil "~A ~A:~A, "
		       uuid
		       (length conn-id)
		       conn-id))
	 (fmsg (if fmsg fmsg ""))
	 (zmsg (make-instance 'zmq:msg))
	 (hlen (length fmsg))
	 (mlen (length msg)))
    (declare (type string fmsg))
    (zmq:msg-init-size zmsg (+ hlen mlen))
    (let ((p (zmq:msg-data-as-is zmsg)))
      (dotimes (i hlen) (setf (cffi:mem-ref p :unsigned-char i) (char-code (aref fmsg i))))
      (etypecase msg
        (string
          (dotimes (i mlen) (setf (cffi:mem-ref p :unsigned-char (+ hlen i)) (char-code (aref msg i)))))
        ((simple-array (unsigned-byte 8) (*))
          (dotimes (i mlen) (setf (cffi:mem-ref p :unsigned-char (+ hlen i)) (aref msg i)))))
      (zmq:send (slot-value connection 'resp) zmsg))))

(defun reply (req msg &optional (connection *current-connection*))
  "Sends a reply to a request object"
  (declare (type connection connection)
	   (type request req))
  (send (request-sender req) (request-conn-id req) msg connection))

(defun reply-json (req data &optional (connection *current-connection*))
  "Sends a reply to request object, encoding the data as JSON"
  (declare (type connection connection))
  (reply req (json:encode-json-to-string data) connection))

(defun reply-http (req body &key (code 200) (status "OK") headers
		   (connection *current-connection*))
  "Sends a reply to a request, prepending an http header"
  (declare (type vector body))
  (reply req (http-response body code status headers) connection))

(defun reply-start-chunk (req &key (code 200) (status "OK")
			  headers (connection *current-connection*))
  "Starts a chunked-transfer reply to a request, prepending an http header
  
  The usage is like this:
  (reply-start-chunk connection request)
  one or more calls to: (reply-a-chunk connection request data)
  (reply-finish-chunk connection request)

  This can be used to send parts of a request seperately when you don't know
  the final length.
  "
  (declare (type list headers)
	   (type connection connection))
  (let ((headers (acons "Transfer-Encoding" "chunked" headers)))
    (declare (type list headers))
    (reply req 
	   (format-http nil code status
		   (mapcar (lambda (x) (list (car x) (cdr x))) headers)
		   "") connection)))

(defun reply-a-chunk (req body &optional (connection *current-connection*))
  "Sends some data to a request that reply-start-chunk has already been called
  on"
  (declare (type string body)
	   (type connection connection))
  (reply req
	 (format-chunk nil (length body) body) connection))

(defun reply-finish-chunk (req &optional (connection *current-connection*))
  "Finishes a request that reply-start-chunk has already been called on."
  (declare (type connection connection))
  (reply-a-chunk req "" connection)
  (reply req +crlf+ connection))

(defun deliver (uuid idents data &optional (connection *current-connection*))
  "Send message to mongrel2 for all idents"
  (declare (type connection connection))
  (multiple-value-bind (idents more)
      (if (<= (length idents) +deliver-max+)
	  idents
	  (values (subseq idents 0 +deliver-max+) (subseq idents +deliver-max+)))
    (send uuid (string-join " " idents) data connection)
    (when more (deliver uuid more data connection))))

(defun deliver-json (uuid idents data &optional (connection *current-connection*))
  "Like deliver, but encode data as JSON"
  (declare (type connection connection))
  (deliver uuid idents (json:encode-json-to-string data) connection))

(defun deliver-http
  (uuid idents body &key (code 200) (status "OK") headers (connection *current-connection*))
  "Like deliver, but prepend an HTTP header"
  (deliver uuid idents (http-response body code status headers) connection))

(defun reply-close (req &optional (connection *current-connection*))
  "Instruct mongrel2 to close a connection"
  (declare (type connection connection))
  (reply req "" connection))

(defun request-closep (req)
  "If this is true, reply-close should  be called at the end of the reply"
  (or (equalp (cdr (assoc :|connection| (request-headers req))) "close")
      (equalp (cdr (assoc :VERSION (request-headers req))) "HTTP/1.0")))

(defun deliver-close (uuid idents &optional (connection *current-connection*))
  "Instruct mongrel2 to close all connections in idents"
  (declare (type connection connection))
  (deliver uuid idents "" connection))
 
(defun simple-test ()
  (with-connection-nowrap (conn
		    "82209006-86FF-4982-B5EA-D1E29E55D483"
		    "tcp://127.0.0.1:9997"
		    "tcp://127.0.0.1:9996")
    (loop
       (let ((req (recv conn)))
         (reply-http req "Hello, World!" :connection conn)))))


(defun example-from-docs (&key (sender-id "82209006-86FF-4982-B5EA-D1E29E55D483") verbose)
  (with-connection (foo
		    sender-id
		    "tcp://127.0.0.1:9997"
		    "tcp://127.0.0.1:9996")
    (loop
       (when verbose (print "WAITING FOR REQUEST"))
       (let ((req (recv)))
	 (when verbose (print "Receive complete"))
	 (when verbose (print req))
	 (cond
	   ((request-disconnectp req)
	    (print "DISCONNECT"))
	    ;(reply-close conn req))
	   ((assoc :|killme| (request-headers req))
	    (print "They want to be killed.")
	    (reply-close req))
	   (t
	    (when verbose (print "Sending Reply"))
	    (reply-http req
			(format nil "<pre>~&SENDER: ~A~&IDENT: ~A~&PATH: ~A~&HEADERS: ~A~&BODY~A</pre>"
				(request-sender req) (request-conn-id req)
				(request-path req)
				(request-headers req)
				(request-body req)))
	    (when (request-closep req)
	      (reply-close req))))))))

(defun example-from-docs-nowrap ()
  (with-connection-nowrap (foo
                     "82209006-86FF-4982-B5EA-D1E29E55D483"
                     "tcp://127.0.0.1:9997"
                     "tcp://127.0.0.1:9996")
                   (loop
                     ;(print "WAITING FOR REQUEST")
                     (let ((req (recv foo)))
                       (cond
                         ((request-disconnectp req)
                          (print "DISCONNECT"))
                         ;(reply-close conn req))
                         ((assoc :|killme| (request-headers req))
                          (print "They want to be killed.")
                          (reply-close foo req))
                         (t
                           (reply-http req
                                       (format nil "<pre>~&SENDER: ~A~&IDENT: ~A~&PATH: ~A~&HEADERS: ~A~&BODY~A</pre>"
                                               (request-sender req) (request-conn-id req)
                                               (request-path req) (request-headers req)
                                               (request-body req))
				       :connection foo)
                           (when (request-closep req)
                             (reply-close foo req))))))))

(defun test-chunking ()
  (with-connection(conn
		    "82209006-86FF-4982-B5EA-D1E29E55D483"
		    "tcp://127.0.0.1:9997"
		    "tcp://127.0.0.1:9996")
    (loop
       ;(print "WAITING FOR REQUEST")
       (let ((req (recv)))
	 (cond
	   ((request-disconnectp req)
	    (print "DISCONNECT"))
	   ((assoc :|killme| (request-headers req))
	    (print "They want to be killed.")
	    (reply-http req ""))
	   (t
	    (reply-start-chunk req)
	    (reply-a-chunk req
			   "<http><head><title>foo</title></head><body>")
	    (reply-a-chunk req
			(format nil "<pre>~&SENDER: ~A~&IDENT: ~A~&"
				(request-sender req) (request-conn-id req)))
	    (reply-a-chunk req
			(format nil "PATH: ~A~&HEADERS: ~A"
				(request-path req) (request-headers req)))
	    (reply-a-chunk req
			(format nil "~&BODY~A</pre>" (request-body req)))
	    (reply-finish-chunk req)))
	 (when (request-closep req)
	   (reply-close req))))))


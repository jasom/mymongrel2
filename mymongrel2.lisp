(declaim (optimize (speed 3)))
(in-package :mymongrel2)

(defparameter *zmq-context* nil)

(defun ensure-zmq-init (&optional (threads 1))
    (unless *zmq-context* (setq *zmq-context* (zmq:init threads))))

(defun zmq-recv-string (socket)
  (let ((query (make-instance 'zmq:msg)))
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

(defmacro with-connection ((conn &rest init-args)
                           &body body)
  "Creates a connection and executes body within it.
   Basically, all your mongrel2 stuff should be wrapped in this."
    `(let ((,conn (make-connection ,@init-args)))
       (unwind-protect
	    (progn ,@body)
	 (close-connection ,conn))))

(defun http-response (body code status headers)
  "Generates an http response; used internally"
  (declare (type string body))
  (let ((headers (acons "Content-Length" (length body) headers)))
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

(defun split-by-char (strng &key (split-char #\Space) (max-splits nil))
  "Returns a list of substrings split by :split-char.  Will only split
  enough times to create :max-splits strings"
  (if strng
    (let ((string strng))
      (declare (type string string)
	       (type (or nil fixnum) max-splits)
	       (type character split-char))
      (loop for i fixnum = 0 then (1+ j)
            for count fixnum = 1 then (1+ count)
		      as j = (position split-char string :start i)
                        when (and max-splits (= count max-splits))
                          collect (subseq string i) into splits
                          and return splits
                        collect (subseq string i j) into splits
                        when (null j) return splits))))

(defun parse-netstring (ns)
  "Parses a netstring into a string, which is basically:
  LENGTH ':' DATA ','

  Note that coding errors are possible here, but that should only a body issue
  since mongrel2 requires ascii in the headers"
  (declare (type string ns))
  (let* ((p (position #\: ns))
         (len (subseq ns 0 p))
         (rest (subseq ns (1+ p)))
         (len (parse-integer len)))
    (unless (eql (char rest len) #\,) (error 'parse-error ns))
    (values (subseq rest 0 len) (subseq rest (1+ len)))))


(defstruct request ()
	   (sender)
	   (conn-id)
	   (path)
	   (headers)
	   (body))


(defun request-disconnectp (req)
  "Returns true if a request should be followed by a disconnect"
  (and (equalp (cdr (assoc :+METHOD+ (request-headers req))) "JSON")
       (equalp (cdr (assoc :type (json:decode-json-from-string (request-body req)))) "disconnect")))


(defun parse (msg)
  "Parses out the mongrel2 message format which is:
  UUID ID PATH SIZE:HEADERS,SIZE:BODY,"
  (destructuring-bind (sender conn-id path rest)
      (split-by-char msg :max-splits 4)
    (multiple-value-bind (headers rest) (parse-netstring rest)
      (let* ((body (parse-netstring rest))
	     (headers (json:decode-json-from-string headers)))
	(make-request :sender sender
		      :conn-id conn-id
		      :path path
		      :headers headers
		      :body body)))))

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
    (zmq:setsockopt resp zmq:IDENTITY sender-id)
    (zmq:connect resp pub-addr)
    connection))

(defun close-connection (c)
  "Closes the connection given to it."
  (zmq:close (slot-value c 'reqs))
  (zmq:close (slot-value c 'resp)))


(defun recv (connection)
  "Receives a single message from mongrel2"
  (parse (zmq-recv-string (slot-value connection 'reqs))))

(defun recv-json (connection)
  "Receives a single message from mongrel2 and decodes as JSON"
  (json:decode-json-from-string (recv connection)))

(defun send (connection uuid conn-id msg)
  "Sends a single message to mongrel2"
  (declare (type vector conn-id))
  (zmq:send (slot-value connection 'resp)
	    (make-instance 'zmq:msg
			   :data (format nil "~A ~A:~A, ~A"
					 uuid
					 (length conn-id)
					 conn-id
					 msg))))

(defun reply (connection req msg)
  "Sends a reply to a request object"
  (send connection (request-sender req) (request-conn-id req) msg))

(defun reply-json (connection req data)
  "Sends a reply to request object, encoding the data as JSON"
  (reply connection req (json:encode-json-to-string data)))

(defun reply-http (connection req body &key (code 200) (status "OK") headers)
  "Sends a reply to a request, prepending an http header"
  (declare (type string body))
  (reply connection req (http-response body code status headers)))

(defun reply-start-chunk (connection req &key (code 200) (status "OK") headers)
  "Starts a chunked-transfer reply to a request, prepending an http header
  
  The usage is like this:
  (reply-start-chunk connection request)
  one or more calls to: (reply-a-chunk connection request data)
  (reply-finish-chunk connection request)

  This can be used to send parts of a request seperately when you don't know
  the final length.
  "
  (declare (type list headers))
  (let ((headers (acons "Transfer-Encoding" "chunked" headers)))
    (declare (type list headers))
    (reply connection req 
	   (format-http nil code status
		   (mapcar (lambda (x) (list (car x) (cdr x))) headers)
		   ""))))

(defun reply-a-chunk (connection req body)
  "Sends some data to a request that reply-start-chunk has already been called
  on"
  (declare (type string body))
  (reply connection req
	 (format-chunk nil (length body) body)))

(defun reply-finish-chunk (connection req)
  "Finishes a request that reply-start-chunk has already been called on."
  (reply-a-chunk connection req "")
  (reply connection req +crlf+))

(defun deliver (connection uuid idents data)
  "Send message to mongrel2 for all idents"
  (send connection uuid (string-join " " idents) data))

(defun deliver-json (connection uuid idents data)
  "Like deliver, but encode data as JSON"
  (deliver connection uuid idents (json:encode-json-to-string data)))

(defun deliver-http
  (connection uuid idents body &key (code 200) (status "OK") headers)
  "Like deliver, but prepend an HTTP header"
  (deliver connection uuid idents (http-response body code status headers)))

(defun reply-close (connection req)
  "Instruct mongrel2 to close a connection"
  (reply connection req ""))

(defun request-closep (req)
  "If this is true, reply-close should  be called at the end of the reply"
  (or (equalp (cdr (assoc :connection (request-headers req))) "close")
      (equalp (cdr (assoc :+version+ (request-headers req))) "HTTP/1.0")))

(defun deliver-close (connection uuid idents)
  "Instruct mongrel2 to close all connections in idents"
  (deliver connection uuid idents ""))
 
(defun simple-test ()
  (with-connection (conn
		    "82209006-86FF-4982-B5EA-D1E29E55D483"
		    "tcp://127.0.0.1:9997"
		    "tcp://127.0.0.1:9996")
    (loop
       (let ((req (recv conn)))
         (reply-http conn req "Hello, World!")))))


(defun example-from-docs ()
  (with-connection (conn
		    "82209006-86FF-4982-B5EA-D1E29E55D483"
		    "tcp://127.0.0.1:9997"
		    "tcp://127.0.0.1:9996")
    (loop
       ;(print "WAITING FOR REQUEST")
       (let ((req (recv conn)))
	 (cond
	   ((request-disconnectp req)
	    (print "DISCONNECT"))
	    ;(reply-close conn req))
	   ((assoc :killme (request-headers req))
	    (print "They want to be killed.")
	    (reply-close conn req))
	   (t
	    (reply-http conn req
			(format nil "<pre>~&SENDER: ~A~&IDENT: ~A~&PATH: ~A~&HEADERS: ~A~&BODY~A</pre>"
				(request-sender req) (request-conn-id req)
				(request-path req) (request-headers req)
				(request-body req)))
	    (when (request-closep req)
	      (reply-close conn req))))))))
         

(defun test-chunking ()
  (with-connection (conn
		    "82209006-86FF-4982-B5EA-D1E29E55D483"
		    "tcp://127.0.0.1:9997"
		    "tcp://127.0.0.1:9996")
    (loop
       ;(print "WAITING FOR REQUEST")
       (let ((req (recv conn)))
	 (cond
	   ((request-disconnectp req)
	    (print "DISCONNECT"))
	   ((assoc :killme (request-headers req))
	    (print "They want to be killed.")
	    (reply-http conn req ""))
	   (t
	    (reply-start-chunk conn req)
	    (reply-a-chunk conn req
			   "<http><head><title>foo</title></head><body>")
	    (reply-a-chunk conn req
			(format nil "<pre>~&SENDER: ~A~&IDENT: ~A~&"
				(request-sender req) (request-conn-id req)))
	    (reply-a-chunk conn req
			(format nil "PATH: ~A~&HEADERS: ~A"
				(request-path req) (request-headers req)))
	    (reply-a-chunk conn req
			(format nil "~&BODY~A</pre>" (request-body req)))
	    (reply-finish-chunk conn req)))
	 (when (request-closep req)
	   (reply-close conn req))))))


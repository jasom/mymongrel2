
(in-package :mymongrel2)

(defparameter *zmq-context* nil)

(defun ensure-zmq-init (&optional (threads 1))
    (unless *zmq-context* (setq *zmq-context* (zmq:init threads))))

(defun zmq-recv-string (socket)
  (let ((query (make-instance 'zmq:msg)))
    (zmq:recv socket query)
    (zmq:msg-data-as-string query)))
(defparameter +http-format+ 
		     "HTTP/1.1 ~A ~A
~:{~A: ~A
~}
~A")

(defmacro with-connection ((conn &rest init-args)
                           &body body)
    `(let ((,conn (make-connection ,@init-args)))
       (unwind-protect
	    (progn ,@body)
	 (close-connection ,conn))))

(defun http-response (body code status headers)
  (let ((headers (acons "Content-Length" (length body) headers)))
    (format nil +http-format+ code status
	    (mapcar (lambda (x) (list (car x) (cdr x))) headers)
	    body)))

(defun string-join (separator sequence)
  (apply #'concatenate 'string
	 (cons (car sequence)
	       (loop for i in (cdr sequence)
		    collect separator
		    collect i))))

(defun split-by-char (strng &key (split-char #\Space) (max-splits nil))
;Returns a list of substrings of string
;divided by 'c'
  (if strng
    (let ((string strng))
      (declare (type vector string))
      (loop for i = 0 then (1+ j)
            for count = 1 then (1+ count)
		      as j = (position split-char string :start i)
                        when (and max-splits (= count max-splits))
                          collect (subseq string i) into splits
                          and return splits
                        collect (subseq string i j) into splits
                        when (null j) return splits))))

(defun parse-netstring (ns)
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
  (if (equalp (assoc :+METHOD+ (request-headers req)) "JSON")
      (equalp (assoc :type (request-headers req)) "disconnect")))


(defun parse (msg)
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
  (zmq:close (slot-value c 'reqs))
  (zmq:close (slot-value c 'resp)))


(defun connection-recv (connection)
  (parse (zmq-recv-string (slot-value connection 'reqs))))

(defun connection-recv-json (connection)
  (json:decode-json-from-string (connection-recv connection)))

(defun connection-send (connection uuid conn-id msg)
  (zmq:send (slot-value connection 'resp)
	    (make-instance 'zmq:msg
			   :data (format nil "~A ~A:~A, ~A"
					 uuid
					 (length conn-id)
					 conn-id
					 msg))))

(defun connection-reply (connection req msg)
  (connection-send connection (request-sender req) (request-conn-id req) msg))

(defun reply-json (connection req data)
  (connection-reply connection req (json:decode-json-from-string data)))

(defun reply-http (connection req body &key (code 200) (status "OK") headers)
  (connection-reply connection req (http-response body code status headers)))

(defun connection-deliver (connection uuid idents data)
  (connection-send connection uuid (string-join " " idents) data))

(defun connection-deliver-json (connection uuid idents data)
  (connection-deliver connection uuid idents (json:encode-json-to-string data)))

(defun connection-deliver-http
    (connection uuid idents body &key (code 200) (status "OK") headers)
  (connection-deliver connection uuid idents (http-response body code status headers)))

(defun connection-close (connection req)
  (connection-reply connection req ""))

(defun connection-deliver-close (connection uuid idents)
  (connection-deliver connection uuid idents ""))
 
(defun simple-test ()
  (with-connection (conn
		    "82209006-86FF-4982-B5EA-D1E29E55D483"
		    "tcp://127.0.0.1:9997"
		    "tcp://127.0.0.1:9996")
    (loop
       (let ((req (connection-recv conn)))
         (reply-http conn req "Hello, World!")))))


(defun example-from-docs ()
  (with-connection (conn
		    "82209006-86FF-4982-B5EA-D1E29E55D483"
		    "tcp://127.0.0.1:9997"
		    "tcp://127.0.0.1:9996")
    (loop
       (print "WAITING FOR REQUEST")
       (let ((req (connection-recv conn)))
	 (cond
	   ((request-disconnectp req)
	    (print "DISCONNECT"))
	   ((assoc :killme (request-headers req))
	    (print "They want to be killed.")
	    (reply-http conn req ""))
	   (t
	    (reply-http conn req
			(format nil "<pre>~&SENDER: ~A~&IDENT: ~A~&PATH: ~A~&HEADERS: ~A~&BODY~A</pre>"
				(request-sender req) (request-conn-id req)
				(request-path req) (request-headers req)
				(request-body req)))))))))
         


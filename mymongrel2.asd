;;;; mymongrel2.asd

(asdf:defsystem #:mymongrel2
  :serial t
  :depends-on (#:zeromq
               #:cl-json)
  :components ((:file "package")
               (:file "mymongrel2")))


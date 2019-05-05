#lang racket

(require racket/contract
         racket/generic
         data/heap
         data/queue
         math/distributions
         threading
         (for-syntax syntax/parse))

(struct Place (contents) #:transparent)
(struct Task (in fn) #:transparent)
(struct Net (places tasks queue [time #:mutable]) #:transparent)


(define (make-place [contents '()])
  (define place (Place (make-queue)))
  (for ((token contents))
    (enqueue! (Place-contents place) token))
  place)

(define (count place)
  (queue-length (Place-contents place)))

(define (push-token! place token)
  (enqueue! (Place-contents place) token))

(define (pop-token! place)
  (dequeue! (Place-contents place)))

(define (ready? task)
  (define places (Task-in task))
  (define counts (foldl (lambda (t counts)
                          (dict-update counts t add1 0))
                        '() places))
  (andmap (match-lambda [(cons p c) (>= (count p) c)]) counts))

(define (ready-tasks net)
  (filter ready? (Net-tasks net)))

(define sim-time (make-parameter 0))

(define (activate! net task)
  (define tokens (for/list ([p (in-list (Task-in task))])
                   (pop-token! p)))
  (define results (parameterize ((sim-time (Net-time net)))
                    (apply (Task-fn task) tokens)))
  (for ([r results])
    (define t (+ (Net-time net) (car r)))
    (heap-add! (Net-queue net) (cons t (cdr r)))))

(define (activate-all! net)
  (let loop ()
    (define tasks (ready-tasks net))
    (if (empty? tasks)
        (void)
        (begin
          (activate! net (car tasks))
          (loop)))))

(define (next-event! net)
  (define evt (heap-min (Net-queue net)))
  (heap-remove-min! (Net-queue net))
  (set-Net-time! net (car evt))
  (apply push-token! (cdr evt)))

(define (step! net)
  (when (empty? (ready-tasks net))
    (next-event! net))
  (activate-all! net))


(define-syntax (petri-net stx)
  (syntax-parse stx
    [(_ x:expr ...) (let ((xs (syntax->list #'(x ...)))
                          (places '())
                          (tasks '()))
                      #`(letrec #,(datum->syntax
                                   stx 
                                   (for/list ((clause xs))
                                     (syntax-case clause (place task)
                                       [(place name token ...)
                                        (set! places (cons #'name places))
                                        #'(name (make-place (list token ...)))]
                                       [(task name ((t p) ...) body ...)
                                        (set! tasks (cons #'name tasks))
                                        #'(name (Task (list p ...)
                                                      (lambda  (t ...)
                                                        body ...)))])))
                          (Net (list #,@(reverse places))
                               (list #,@(reverse tasks))
                               (make-heap (lambda (a b)
                                            (<= (car a) (car b))))
                               0)))]))

(define n (petri-net
           (place A 'a 'b)
           (place B 'b)
           (task t1 ((a A)
                     (b B))
                 (printf "t1 @ ~a~%" (sim-time))
                 (list (list 5 A 'a) (list 5 B 'b)))
           (task t2 ((a A))
                 (printf "t2 @ ~a~%" (sim-time))
                 (list (list 6 A 'a)))))



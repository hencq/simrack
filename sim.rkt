#lang racket

(require racket/contract
         racket/generator
         racket/generic
         data/heap
         data/queue
         math/distributions
         threading)

(provide
 (contract-out [make-sim (-> sim?)]
               [make-event (->* [sim?] [(listof (-> any))] event?)]
               [add-callback! (-> event? (-> any) any)]
               [trigger! (->* [event?] [number?] any)]
               [step! (-> sim? any)]
               [run! (-> sim? #:until number? any)]
               [wait (-> sim? number? event?)]
               [process (-> sim? (-> sim? (-> any)) event?)]
               [make-resource (-> sim? number? resource?)]
               [request (-> resource? event:request?)]
               [release (-> event:request? any)]))

(struct sim ([time #:mutable] queue idgen) #:transparent)
(struct schedule-entry (time id event) #:transparent)

(define (make-sim)
  (define (event<=? x y)
    (and 
     (<= (event-scheduled x) (event-scheduled y))
     (<= (event-id x) (event-id y))))
  (define idgen (generator ()
                           (let loop ([id 0])
                             (yield id)
                             (loop (add1 id)))))
  (sim 0 (make-heap event<=?) idgen))

(define-generics cancelable
  (cancel cancelable))

(struct event (sim [callbacks #:mutable] [completed? #:mutable] [scheduled #:mutable] [id #:mutable]) #:transparent)

(define (make-event sim [callbacks '()])
  (event sim callbacks #f #f #f))

(define (add-callback! evt cb)
  (set-event-callbacks! evt (cons cb (event-callbacks evt))))

(define (trigger! event [time #f])
  (when (and (not (triggered? event)) (not (completed? event)))
    (define sim (event-sim event))
    (set! time (or time (sim-time sim)))
    (set-event-scheduled! event (or time (sim-time sim)))
    (set-event-id! event ((sim-idgen sim)))
    (heap-add! (sim-queue sim) event)
    event))

(define (triggered? event)
  (if (event-scheduled event) #t #f))

(define completed? event-completed?)

(define (cancel-event! event)
  (when (and (triggered? event) (not (completed? event)))
    (define sim (event-sim event))
    (heap-remove! (sim-queue sim) event)))

(define (step! sim)
  (define queue (sim-queue sim))
  (when (> (heap-count queue) 0)
    (let* ([evt (heap-min queue)]
           [callbacks (event-callbacks evt)]
           [new-time (event-scheduled evt)])
      (heap-remove-min! queue)
      (when (not (event-completed? evt))
        (set-sim-time! sim new-time)
        (for ([cb callbacks])
          (cb))
        (set-event-completed?! evt #t)))))

(define (run! sim #:until [until #f])
  (let loop ()
    (cond
      [(= (heap-count (sim-queue sim)) 0) (void)]
      [(and (number? until) (>= (sim-time sim) until)) (void)]
      [else (begin
              (step! sim)
              (loop))])))

(define (wait sim delay)
  (define cur-time (sim-time sim))
  (define evt (make-event sim))
  (trigger! evt (+ cur-time delay)))

(define (process sim fun)
  (define proc-evt (make-event sim))
  (define gen (fun sim))
  (define (cb)
    (define res (gen))
    (if (event? res)
        (add-callback! res cb)
        (trigger! proc-evt)))
  (trigger! (make-event sim (list cb)))
  proc-evt)

(struct resource (sim cap queue [users #:mutable]) #:transparent)
(struct event:request event (resource))

(define (make-resource sim cap)
  (resource sim cap (make-queue) '()))

(define (request res)
  (define sim (resource-sim res))
  (define put (event:request sim '() #f #f #f res))
  (if (< (length (resource-users res)) (resource-cap res))
      (begin
        (set-resource-users! res (cons put (resource-users res)))
        (trigger! put))
      (begin
        (enqueue! (resource-queue res) put)
        put)))

(define (release req)
  (define res (event:request-resource req))
  (define sim (resource-sim res))
  (if (memq req (resource-users res))
      (begin
        (set-resource-users! res (remq req (resource-users res) ))
        (when (not (queue-empty? (resource-queue res)))
          (define next (dequeue! (resource-queue res)))
          (set-resource-users! res (cons next (resource-users res)))
          (trigger! next)))
      (raise-argument-error 'request "Request is not in use by resource")))

(define-syntax (with-resource stx)
  (syntax-case stx ()
    [(_ (req res) body ...) #'(let ([req (request res)])
                                body ...
                                (release req))]))

(define-syntax (swap! stx)
  (syntax-case stx ()
    [(_ a b) #'(begin
                 (define tmp #f)
                 (set! tmp a)
                 (set! a b)
                 (set! b tmp))]))

(define (any . evts)
  (define any-evt (make-event (event-sim (first evts))))
  (define done? #f)
  (define (cb)
    (when (not done?)
      (set! done? #t)
      (trigger! any-evt)))
  (for ([e evts])
    (add-callback! e cb))
  any-evt)


(define mysim (make-sim))

;; (define ((cust name) sim)
;;   (generator ()
;;              (printf "~a entering @~a~%" name (sim-time sim))
;;              (define req (request counter))
;;              (yield req)
;;              (printf "~a at the counter @~a~%" name (sim-time sim))
;;              (yield (wait sim 5))
;;              (release req)
;;              (printf "~a leaving @~a~%" name (sim-time sim))))

(define ((cust2 name) sim)
  (generator ()
             (printf "~a entering @~a~%" name (sim-time sim))
             (with-resource [req counter]
                   (yield req)
                   (printf "~a at the counter @~a~%" name (sim-time sim))
                   (yield (wait sim 5)))
             (printf "~a leaving @~a~%" name (sim-time sim))))

(define counter (make-resource mysim 1))

(define adam (process mysim (cust2 "Adam")))
(define bob (process mysim (cust2 "Bob")))

import logging
from datetime import datetime, timedelta

from google.appengine.api import users
from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.ext.webapp import template

import model
from util import retry

log = logging.getLogger(__name__)
# log.setLevel(logging.DEBUG)
logging.basicConfig()


chunk_size = 100
limit_time = timedelta(0, 3) # 3 sec

sieve_root = model.Root.get_or_insert(key_name='sieve')
prime_root = model.Root.get_or_insert(key_name='prime')


def transactional(f):
    def wrapped(*args, **kw):
        return db.run_in_transaction(f, *args, **kw)
    return wrapped


def get_next_prime():
    log.debug("get_next_prime()")
    @transactional
    def get_ownership(prime):
        log.debug("get_ownereship()")
        key = prime.key()
        last_assigned_at = prime.last_assigned_at
        # get the current entity in this transaction
        prime = model.Prime.get(key)
        if prime.last_assigned_at != last_assigned_at:
            return None
        prime.last_assigned_at = datetime.now()
        prime.put()
        return prime
    while retry(10, raise_if_failed=True):
        all_primes = model.Prime.all()
        primes = all_primes.order("last_assigned_at").order("number").fetch(1)
        log.debug("primes retrieved")
        if primes:
            prime = get_ownership(primes[0])
            if prime:
                return prime
        elif not all_primes.fetch(1):
            log.debug("call Prime.ensure_number()")
            sentinel = model.Prime.ensure_number(0, parent=prime_root)
        else:
            pass


@transactional
def delete_sieve(sieve):
    log.debug("delete_sieve()")
    # make sure sieve exists
    if model.Sieve.get(sieve.key()) is None:
        raise KeyError(sieve.number)
    sieve.delete()
    return sieve.number


def pop_sieves():
    log.debug("pop_sieves()")
    while retry(10, raise_if_failed=True):
        all_sieves = model.Sieve.all()
        sieves = all_sieves.order("number").fetch(1)
        if sieves:
            try:
                return delete_sieve(sieves[0])
            except (KeyError, db.TransactionFailedError):
                pass
        else:
            @transactional
            def init_sieves():
                for number in xrange(2, chunk_size):
                    model.Sieve(number=number, parent=sieve_root).put()
            init_sieves()


def get_max_sieve():
    log.debug("get_max_sieve()")
    while retry(10, raise_if_failed=True):
        all_sieves = model.Sieve.all()
        sieves = all_sieves.order("-number").fetch(1)
        if sieves:
            return sieves[0].number


def find_prime():
    log.debug("find_prime()")
    prime = get_next_prime()
    if prime.number == 0:
        smallest_number = pop_sieves()
        new_prime = model.Prime(number=smallest_number, parent=prime_root)
        new_prime.put()
        prime = new_prime
    else:
        new_prime = None

    max_sieve = get_max_sieve()

    n = prime.number * 2
    if n > max_sieve:
        # extend the range of sieves
        for i in xrange(min(n - max_sieve, chunk_size)):
            model.Sieve.get_or_insert(number=max_sieve+1, parent=sieve_root)
            max_sieve += 1
    else:
        # sieve them
        log.debug("sieve them: %d %d" % (n, max_sieve))
        while n < max_sieve:
            sieve = model.Sieve.get_by_number(n, parent=sieve_root)
            if sieve:
                try:
                    log.debug("delete_sieve(%d)" % sieve.number)
                    delete_sieve(sieve)
                except (KeyError, db.TransactionFailedError):
                    pass
            else:
                log.warn("%d not found in sieves" % n)
            n += prime.number

    return new_prime


def get_prime():
    start_time = datetime.now()
    n = 0
    while True:
        prime = find_prime()
        if prime:
            return prime
        elapsed_time = datetime.now() - start_time
        n += 1
        if elapsed_time / n * (n + 1) > limit_time:
            return None


class IndexPage(webapp.RequestHandler):
    def get(self):
        log.debug("IndexPage.get()")
        user = users.get_current_user()
        self.response.out.write(
            template.render('index.html', {'user': user}))


class LoginPage(webapp.RequestHandler):
    def get(self):
        log.debug("LoginPage.get()")
        user = users.get_current_user()
        if user:
            self.redirect("/")
        else:
            self.redirect(users.create_login_url("/"))


class LogoutPage(webapp.RequestHandler):
    def get(self):
        log.debug("LogoutPage.get()")
        user = users.get_current_user()
        if user:
            self.redirect(users.create_logout_url("/"))
        else:
            self.redirect("/")


class ListPage(webapp.RequestHandler):
    def get(self):
        log.debug("ListPage.get()")
        primes = model.Prime.all().order("-number").fetch(10)
        primes = [{'number': p.number, 'owner': p.owner or "ANONYMOUS"} for p in primes]
        self.response.out.write(
            template.render('list.html', {'primes': primes}))


class GetPrimePage(webapp.RequestHandler):
    def get(self):
        log.debug("GetPrimePage.get()")
        prime = get_prime()
        if prime:
            prime.owner = users.get_current_user()
            prime.put()
        self.response.out.write(
            template.render('prime.html', {'prime': prime}))


class YourPrimesPage(webapp.RequestHandler):
    def get(self):
        log.debug("YourPrimesPage.get()")
        user = users.get_current_user()
        if user:
            primes = model.Prime.all().filter("owner =", user).order("number")
            primes = [p.number for p in primes]
            self.response.out.write(
                template.render('yourprimes.html', {'primes': primes}))
        else:
            self.redirect(users.create_login_url(self.request.uri))


def main():
    import wsgiref.handlers
    app = webapp.WSGIApplication(
        [('/', IndexPage),
         ('/login', LoginPage),
         ('/logout', LogoutPage),
         ('/list', ListPage),
         ('/get_prime', GetPrimePage),
         ('/yourprimes', YourPrimesPage),
         ],
        debug=True)
    wsgiref.handlers.CGIHandler().run(app)


if __name__ == "__main__":
    main()

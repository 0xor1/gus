package gus

import(
	`log`
	`sync`
	`github.com/0xor1/sus`
	`github.com/qedus/nds`
	`golang.org/x/net/context`
	`google.golang.org/appengine/datastore`
)

type ContextFactory func() context.Context

// Creates and configures a store that stores entities in Google AppEngines memcache and datastore.
// github.com/qedus/nds is used for strongly consistent automatic caching.
func NewGaeStore(kind string, ctxFactory ContextFactory, idf sus.IdFactory, vf sus.VersionFactory) sus.Store {
	var tranCtx context.Context
	var mtx sync.Mutex

	getKey := func(ctx context.Context, id string) *datastore.Key {
		return datastore.NewKey(ctx, kind, id, 0, nil)
	}

	getMulti := func(ids []string) (vs []sus.Version, err error) {
		count := len(ids)
		vs = make([]sus.Version, count, count)
		ks := make([]*datastore.Key, count, count)
		for i := 0; i < count; i++ {
			vs[i] = vf()
			ks[i] = getKey(tranCtx, ids[i])
		}
		log.Println(vs)
		log.Println(ks)
		err = nds.GetMulti(tranCtx, ks, vs)
		log.Println(err)
		return
	}

	putMulti := func(ids []string, vs []sus.Version) (err error) {
		count := len(ids)
		ks := make([]*datastore.Key, count, count)
		for i := 0; i < count; i++ {
			ks[i] = getKey(tranCtx, ids[i])
		}
		log.Println(vs)
		log.Println(ks)
		_, err = nds.PutMulti(tranCtx, ks, vs)
		log.Println(err)
		return
	}

	delMulti := func(ids []string) error {
		count := len(ids)
		ks := make([]*datastore.Key, count, count)
		for i := 0; i < count; i++ {
			ks[i] = getKey(tranCtx, ids[i])
		}
		return nds.DeleteMulti(tranCtx, ks)
	}

	isNonExtantError := func(err error) bool {
		return err == datastore.ErrNoSuchEntity
	}

	rit := func(tran sus.Transaction) error {
		return nds.RunInTransaction(ctxFactory(), func(ctx context.Context)error{
			//this mutex might be completely unnecessary, it might only matter that transactions have a context, not that they have a unique context
			mtx.Lock()
			defer func(){
				tranCtx = nil
				mtx.Unlock()
			}()
			tranCtx = ctx
			return tran()
		}, &datastore.TransactionOptions{XG:true})
	}

	return sus.NewStore(getMulti, putMulti, delMulti, idf, vf, isNonExtantError,rit)
}
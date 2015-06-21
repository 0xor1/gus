package gus

import(
	`fmt`
	`testing`
	`net/http`
	`appengine/aetest`
	`github.com/0xor1/sus`
	`golang.org/x/net/context`
	`google.golang.org/appengine`
	`github.com/stretchr/testify/assert`
)

func Test_GaeStore_Create(t *testing.T){
	fgs := newFooGaeStore()
	id, foo, err := fgs.Create()
	assert.Equal(t, `1`, id, `id should be valid`)
	assert.Equal(t, 0, foo.GetVersion(), `Version should be initialised to 0`)
	assert.Nil(t, err, `err should be nil`)
}

type foo struct{
	sus.Version
}

func newFooGaeStore() *fooGaeStore {
	ctxFactory := func()context.Context{
		ctx, _ := aetest.NewContext(nil)
		return appengine.NewContext(ctx.Request().(*http.Request))
	}
	idSrc := 0
	idf := func() string {
		idSrc++
		return fmt.Sprintf(`%d`, idSrc)
	}
	vf := func() sus.Version {
		return &foo{sus.NewVersion()}
	}
	return &fooGaeStore{
		inner: NewGaeStore(`foo`, ctxFactory, idf, vf),
	}
}

type fooGaeStore struct {
	inner sus.Store
}

func (fgs *fooGaeStore) Create() (id string, f *foo, err error) {
	id, v, err := fgs.inner.Create()
	if v != nil {
		f = v.(*foo)
	}
	return
}

func (fgs *fooGaeStore) CreateMulti(count uint) (ids []string, fs []*foo, err error) {
	ids, vs, err := fgs.inner.CreateMulti(count)
	if vs != nil {
		count := len(vs)
		fs = make([]*foo, count, count)
		for i := 0; i < count; i++ {
			fs[i] = vs[i].(*foo)
		}
	}
	return
}

func (fgs *fooGaeStore) Read(id string) (f *foo, err error) {
	v, err := fgs.inner.Read(id)
	if v != nil {
		f = v.(*foo)
	}
	return
}

func (fgs *fooGaeStore) ReadMulti(ids []string) (fs []*foo, err error) {
	vs, err := fgs.inner.ReadMulti(ids)
	if vs != nil {
		count := len(vs)
		fs = make([]*foo, count, count)
		for i := 0; i < count; i++ {
			fs[i] = vs[i].(*foo)
		}
	}
	return
}

func (fgs *fooGaeStore) Update(id string, f *foo) (err error) {
	return fgs.inner.Update(id, f)

}

func (fgs *fooGaeStore) UpdateMulti(ids []string, fs []*foo) (err error) {
	if fs != nil {
		count := len(fs)
		vs := make([]sus.Version, count, count)
		for i := 0; i < count; i++ {
			vs[i] = sus.Version(fs[i])
		}
		err = fgs.inner.UpdateMulti(ids, vs)
	}
	return
}

func (fgs *fooGaeStore) Delete(id string) (err error) {
	return fgs.inner.Delete(id)
}

func (fgs *fooGaeStore) DeleteMulti(ids []string) (err error) {
	return fgs.inner.DeleteMulti(ids)
}


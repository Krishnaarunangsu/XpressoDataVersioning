import pypachy

client = pypachy.PfsClient
client.create_repo('test')
client.list_file(('my_repo', 'branch'), '/')    # tuple
client.list_file('my_repo/commit_id', '/')      # string
c = client.list_commit('my_repo')[0]            # get some commit
client.list_file(c, '/')                        # and use it directly

with client.commit('test', 'master') as c:
     client.put_file_bytes(c, '/dir_a/data', b'DATA')
     client.put_file_url(c, '/dir_b/icon.png', 'http://www.pearl-guide.com/forum/images/smilies/biggrin.png')
     client.get_files('test/master', '/', recursive=True)

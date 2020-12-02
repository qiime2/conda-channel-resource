FROM stilliard/pure-ftpd:hardened

ENV FTP_MAX_CLIENTS=50

CMD /run.sh \
  -c 75 \
  -C 20 \
  -l puredb:/etc/pure-ftpd/pureftpd.pdb \
  -E \
  -j \
  -R \
  -P $PUBLICHOST \
  -p 30000:30099 \
  -O w3c:/var/log/pure-ftpd/transfer.log \
  -d -d \
  --tls=1

EXPOSE 21 30000-30099

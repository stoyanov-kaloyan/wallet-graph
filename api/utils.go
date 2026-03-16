package main

func walletURI(addr string) string {
	return "<urn:wallet-graph:wallet:" + addr + ">"
}

func txURI(hash string) string {
	return "<urn:wallet-graph:tx:" + hash + ">"
}

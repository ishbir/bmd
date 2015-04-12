package bmec_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/monetas/bmd/bmec"
)

// Test 1: Encryption and decryption
func TestCipheringBasic(t *testing.T) {
	privkey, err := bmec.NewPrivateKey(bmec.S256())
	if err != nil {
		t.Fatal("failed to generate private key")
	}

	in := []byte("Hey there dude. How are you doing? This is a test.")

	out, err := bmec.Encrypt(privkey.PubKey(), in)
	if err != nil {
		t.Fatal("failed to encrypt:", err)
	}

	dec, err := bmec.Decrypt(privkey, out)
	if err != nil {
		t.Fatal("failed to decrypt:", err)
	}

	if !bytes.Equal(in, dec) {
		t.Error("decrypted data doesn't match original")
	}
}

// Test 2: Byte compatibility with Pyelliptic
func TestCiphering(t *testing.T) {
	pb, _ := hex.DecodeString("fe38240982f313ae5afb3e904fb8215fb11af1200592b" +
		"fca26c96c4738e4bf8f")
	privkey, _ := bmec.PrivKeyFromBytes(bmec.S256(), pb)

	in := []byte("This is just a test.")
	out, _ := hex.DecodeString("b0d66e5adaa5ed4e2f0ca68e17b8f2fc02ca002009e3" +
		"3487e7fa4ab505cf34d98f131be7bd258391588ca7804acb30251e71a04e0020ecf" +
		"df0f84608f8add82d7353af780fbb28868c713b7813eb4d4e61f7b75d7534dd9856" +
		"9b0ba77cf14348fcff80fee10e11981f1b4be372d93923e9178972f69937ec850ed" +
		"6c3f11ff572ddd5b2bedf9f9c0b327c54da02a28fcdce1f8369ffec")

	dec, err := bmec.Decrypt(privkey, out)
	if err != nil {
		t.Fatal("failed to decrypt:", err)
	}

	if !bytes.Equal(in, dec) {
		t.Error("decrypted data doesn't match original")
	}
}

func TestCipheringErrors(t *testing.T) {
	privkey, err := bmec.NewPrivateKey(bmec.S256())
	if err != nil {
		t.Fatal("failed to generate private key")
	}

	tests1 := []struct {
		ciphertext []byte // input ciphertext
		err        error  // expected error
	}{
		{bytes.Repeat([]byte{0x00}, 133), bmec.ErrInputTooShort},
		{bytes.Repeat([]byte{0x00}, 134), bmec.ErrUnsupportedCurve},
		{bytes.Repeat([]byte{0x02, 0xCA}, 134), bmec.ErrInvalidXLength},
		{bytes.Repeat([]byte{0x02, 0xCA, 0x00, 0x20}, 134), bmec.ErrInvalidYLength},
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // IV
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x02, 0xCA, 0x00, 0x20, // curve and X length
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // X
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x20, // Y length
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Y
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ciphertext
			// padding not aligned to 16 bytes
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // MAC
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		}, bmec.ErrInvalidPadding},
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // IV
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x02, 0xCA, 0x00, 0x20, // curve and X length
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // X
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x20, // Y length
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Y
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ciphertext
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // MAC
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		}, bmec.ErrInvalidMAC},
	}

	for i, test := range tests1 {
		_, err = bmec.Decrypt(privkey, test.ciphertext)
		if err != test.err {
			t.Errorf("Decrypt #%d wrong error got: %v, want: %v", i, err, test.err)
		}
	}

	// test error from removePKCSPadding
	tests2 := []struct {
		in []byte // input data
	}{
		{bytes.Repeat([]byte{0x11}, 17)},
		{bytes.Repeat([]byte{0x07}, 15)},
	}
	for i, test := range tests2 {
		_, err = bmec.TstRemovePKCSPadding(test.in)
		if err != bmec.ErrInvalidPadding {
			t.Errorf("removePKCSPadding #%d wrong error got: %v, want: %v", i,
				err, bmec.ErrInvalidPadding)
		}
	}
}
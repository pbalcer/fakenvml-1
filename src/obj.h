/*
 * Copyright (c) 2014, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * obj.h -- internal definitions for obj module
 */

/* attributes of the obj memory pool format for the pool header */
#define	OBJ_HDR_SIG "OBJPOOL"	/* must be 8 bytes including '\0' */
#define	OBJ_FORMAT_MAJOR 1
#define	OBJ_FORMAT_COMPAT 0x0000
#define	OBJ_FORMAT_INCOMPAT 0x0000
#define	OBJ_FORMAT_RO_COMPAT 0x0000

struct pmemobjpool {
	struct pool_hdr hdr;	/* memory pool header */

	/* root info for on-media format... */

	/* some run-time state, allocated out of memory pool... */
	void *addr;		/* mapped region */
	size_t size;		/* size of mapped region */

	/* for the fake implementation... */
	PMEMmutex rootlock;
	void *rootdirect;
};

/* alignment of every object */
#define	PMEMOID_INTERNAL_ALIGN 256

/* info kept by the library for each allocated object */
struct objheader {
	uint64_t size;		/* requested object size */
	uint64_t actual_size;	/* actual object size */
	uint64_t flags;
	uint64_t unused[5];
};
